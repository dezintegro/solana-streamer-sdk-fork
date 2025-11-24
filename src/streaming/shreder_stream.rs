use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc;

use crate::common::AnyResult;
use crate::protos::shreder::{SubscribeRequestFilterTransactions, SubscribeTransactionsRequest};
use crate::streaming::common::{EventProcessor, SubscriptionHandle};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{Protocol, UnifiedEvent};
use crate::streaming::shreder::pool::factory;
use log::error;

use super::ShrederGrpc;

impl ShrederGrpc {
    /// Subscribe to Shreder events with transaction filtering
    ///
    /// # Arguments
    ///
    /// * `protocols` - List of protocols to monitor (PumpFun, Raydium, etc.)
    /// * `account_filters` - Optional account filters for transaction subscription
    /// * `bot_wallet` - Optional bot wallet for filtering
    /// * `event_type_filter` - Optional filter for specific event types
    /// * `callback` - Callback function to handle events
    ///
    /// # Example
    ///
    /// ```ignore
    /// let filters = vec!["account1".to_string(), "account2".to_string()];
    /// shreder.shreder_subscribe(
    ///     vec![Protocol::PumpFun],
    ///     Some(filters),
    ///     None,
    ///     None,
    ///     |event| { println!("Event: {:?}", event); }
    /// ).await?;
    /// ```
    pub async fn shreder_subscribe<F>(
        &self,
        protocols: Vec<Protocol>,
        account_filters: Option<Vec<String>>,
        bot_wallet: Option<Pubkey>,
        event_type_filter: Option<EventTypeFilter>,
        callback: F,
    ) -> AnyResult<()>
    where
        F: Fn(Box<dyn UnifiedEvent>) + Send + Sync + 'static,
    {
        // Stop any active subscription
        self.stop().await;

        let mut metrics_handle = None;
        // Start automatic performance monitoring (if enabled)
        if self.config.enable_metrics {
            metrics_handle = self.metrics_manager.start_auto_monitoring().await;
        }

        // Create event processor
        let mut event_processor =
            EventProcessor::new(self.metrics_manager.clone(), self.config.clone());
        event_processor.set_protocols_and_event_type_filter(
            super::common::EventSource::Shred, // Shreder uses similar processing to Shred
            protocols,
            event_type_filter,
            self.config.backpressure.clone(),
            Some(Arc::new(callback)),
        );

        // Prepare subscription request
        let mut transactions_map = HashMap::new();

        // Create filter based on provided accounts
        let filter = SubscribeRequestFilterTransactions {
            account_include: account_filters.clone().unwrap_or_default(),
            account_exclude: vec![],
            account_required: account_filters.unwrap_or_default(),
        };

        transactions_map.insert("shreder_subscription".to_string(), filter);

        let initial_request = SubscribeTransactionsRequest {
            transactions: transactions_map,
        };

        // Create streaming channel
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(initial_request)?;
        drop(tx); // Close sender to complete the stream

        // Start stream processing
        let mut client = (*self.shreder_client).clone();
        let request = tonic::Request::new(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }));
        let mut stream = client.subscribe_transactions(request).await?.into_inner();

        let event_processor_clone = event_processor.clone();
        let stream_task = tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => {
                        if let Some(update_tx) = msg.transaction {
                            if let Some(proto_tx) = update_tx.transaction {
                                // Convert protobuf transaction to Solana VersionedTransaction
                                match convert_proto_to_versioned_transaction(&proto_tx) {
                                    Ok(versioned_tx) => {
                                        let shreder_transaction = factory::create_shreder_transaction_pooled(
                                            versioned_tx,
                                            update_tx.slot,
                                            get_high_perf_clock(),
                                            msg.created_at,
                                        );

                                        // Convert to TransactionWithSlot for compatibility with EventProcessor
                                        let transaction_with_slot = crate::streaming::shred::TransactionWithSlot {
                                            transaction: shreder_transaction.transaction,
                                            slot: shreder_transaction.slot,
                                            recv_us: shreder_transaction.recv_us,
                                        };

                                        // Process transaction with metrics
                                        if let Err(e) = event_processor_clone
                                            .process_shred_transaction_with_metrics(
                                                transaction_with_slot,
                                                bot_wallet,
                                            )
                                            .await
                                        {
                                            error!("Error processing transaction: {e:?}");
                                        }
                                    }
                                    Err(e) => {
                                        error!("Error converting transaction: {e:?}");
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        error!("Stream error: {error:?}");
                        break;
                    }
                }
            }
        });

        // Save subscription handle
        let subscription_handle = SubscriptionHandle::new(stream_task, None, metrics_handle);
        let mut handle_guard = self.subscription_handle.lock().await;
        *handle_guard = Some(subscription_handle);

        Ok(())
    }
}

/// Convert protobuf Transaction to Solana VersionedTransaction
fn convert_proto_to_versioned_transaction(
    proto_tx: &crate::protos::shreder::Transaction,
) -> AnyResult<solana_sdk::transaction::VersionedTransaction> {
    use solana_sdk::message::{
        v0::MessageAddressTableLookup as SolanaMessageAddressTableLookup,
        MessageHeader as SolanaMessageHeader, VersionedMessage,
    };
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use solana_sdk::message::legacy::Message as LegacyMessage;

    // Parse message if present
    let message = proto_tx
        .message
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing message in transaction"))?;

    // Parse header
    let header = message
        .header
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing header in message"))?;

    let solana_header = SolanaMessageHeader {
        num_required_signatures: header.num_required_signatures as u8,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
    };

    // Parse account keys
    let account_keys: Result<Vec<Pubkey>, _> = message
        .account_keys
        .iter()
        .map(|key| Pubkey::try_from(key.as_slice()))
        .collect();
    let account_keys = account_keys?;

    // Parse recent blockhash
    let mut hash_array = [0u8; 32];
    let len = message.recent_blockhash.len().min(32);
    hash_array[..len].copy_from_slice(&message.recent_blockhash[..len]);
    let recent_blockhash = solana_sdk::hash::Hash::new_from_array(hash_array);

    // Parse instructions
    use solana_sdk::message::compiled_instruction::CompiledInstruction as SolanaCompiledInstruction;
    let instructions: Vec<SolanaCompiledInstruction> = message
        .instructions
        .iter()
        .map(|ix| SolanaCompiledInstruction {
            program_id_index: ix.program_id_index as u8,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        })
        .collect();

    // Create versioned message
    let versioned_message = if message.versioned && !message.address_table_lookups.is_empty() {
        // V0 message with address table lookups
        let address_table_lookups: Vec<SolanaMessageAddressTableLookup> = message
            .address_table_lookups
            .iter()
            .map(|lookup| {
                let account_key = Pubkey::try_from(lookup.account_key.as_slice())
                    .unwrap_or_default();
                SolanaMessageAddressTableLookup {
                    account_key,
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                }
            })
            .collect();

        let v0_message = solana_sdk::message::v0::Message {
            header: solana_header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        };
        VersionedMessage::V0(v0_message)
    } else {
        // Legacy message
        let legacy_message = solana_sdk::message::legacy::Message {
            header: solana_header,
            account_keys,
            recent_blockhash,
            instructions,
        };
        VersionedMessage::Legacy(legacy_message)
    };

    // Parse signatures
    let signatures: Result<Vec<Signature>, _> = proto_tx
        .signatures
        .iter()
        .map(|sig| Signature::try_from(sig.as_slice()))
        .collect();
    let signatures = signatures?;

    Ok(solana_sdk::transaction::VersionedTransaction {
        signatures,
        message: versioned_message,
    })
}
