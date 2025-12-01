use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::{StreamExt, SinkExt};
use futures::channel::mpsc;
use solana_sdk::{
    hash::Hash,
    message::{
        v0::MessageAddressTableLookup as SolanaMessageAddressTableLookup,
        compiled_instruction::CompiledInstruction as SolanaCompiledInstruction,
        MessageHeader as SolanaMessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use anyhow::anyhow;

use crate::common::AnyResult;
use crate::protos::arpc::{SubscribeRequest, SubscribeResponseTransaction};
use crate::streaming::common::{EventProcessor, SubscriptionHandle};
use crate::streaming::event_parser::common::filter::EventTypeFilter;
use crate::streaming::event_parser::common::high_performance_clock::get_high_perf_clock;
use crate::streaming::event_parser::{Protocol, UnifiedEvent};
use crate::streaming::shred::pool::factory;
use crate::streaming::arpc::types::TransactionFilter;
use log::{error, info};

use super::ArpcGrpc;

impl ArpcGrpc {
    /// Subscribe to ARPC transaction stream with support for dynamic updates
    ///
    /// Supports multiple transaction filters with unique keys, similar to Yellowstone gRPC.
    /// Each filter will be assigned a unique key in the format "client_{index}".
    ///
    /// # Parameters
    /// * `protocols` - List of protocols to parse events from
    /// * `bot_wallet` - Optional bot wallet for filtering
    /// * `event_type_filter` - Optional event type filter
    /// * `transaction_filters` - Vector of transaction filters to apply
    /// * `callback` - Callback function to handle events
    ///
    /// # Example
    /// ```no_run
    /// use solana_streamer_sdk::streaming::arpc::TransactionFilter;
    /// use solana_streamer_sdk::streaming::event_parser::Protocol;
    ///
    /// let transaction_filter = TransactionFilter {
    ///     account_include: vec!["account1".to_string()],
    ///     account_exclude: vec![],
    ///     account_required: vec![],
    /// };
    ///
    /// client.arpc_subscribe(
    ///     vec![Protocol::PumpSwap],
    ///     None,
    ///     None,
    ///     vec![transaction_filter],
    ///     |event| { println!("Event: {:?}", event); }
    /// ).await?;
    /// ```
    pub async fn arpc_subscribe<F>(
        &self,
        protocols: Vec<Protocol>,
        bot_wallet: Option<Pubkey>,
        event_type_filter: Option<EventTypeFilter>,
        transaction_filters: Vec<TransactionFilter>,
        callback: F,
    ) -> AnyResult<()>
    where
        F: Fn(Box<dyn UnifiedEvent>) + Send + Sync + 'static,
    {
        // Check if already subscribed
        if self
            .active_subscription
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(anyhow!("Already subscribed. Use update_subscription() to modify filters"));
        }

        let mut metrics_handle = None;
        // Start automatic performance monitoring (if enabled)
        if self.config.enable_metrics {
            metrics_handle = self.metrics_manager.start_auto_monitoring().await;
        }

        // Create event processor
        let mut event_processor =
            EventProcessor::new(self.metrics_manager.clone(), self.config.clone());
        event_processor.set_protocols_and_event_type_filter(
            super::common::EventSource::Shred,
            protocols,
            event_type_filter,
            self.config.backpressure.clone(),
            Some(Arc::new(callback)),
        );

        // Generate filters with unique keys
        let filters = self.generate_filters(transaction_filters);

        let request = SubscribeRequest {
            transactions: filters,
            ping_id: None, // We'll implement ping later if needed
        };

        // Save current request for updates
        *self.current_request.write().await = Some(request.clone());

        // Create control channel for subscription updates
        let (control_tx, mut control_rx) = mpsc::channel::<SubscribeRequest>(100);
        *self.control_tx.lock().await = Some(control_tx);

        // Create channel for sending requests to gRPC stream
        let (mut subscribe_tx, subscribe_rx) = mpsc::channel::<SubscribeRequest>(100);

        // Send initial request
        subscribe_tx.send(request).await
            .map_err(|e| anyhow!("Failed to send initial request: {}", e))?;

        // Start streaming
        let mut client = (*self.arpc_client).clone();
        let mut stream = client.subscribe(subscribe_rx).await?.into_inner();

        let event_processor_clone = event_processor.clone();
        let stream_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    message = stream.next() => {
                        match message {
                            Some(Ok(response)) => {
                                // Process transaction if present
                                if let Some(tx) = response.transaction {
                                    match convert_arpc_to_versioned_transaction(&tx) {
                                        Ok(versioned_tx) => {
                                            let transaction_with_slot =
                                                factory::create_transaction_with_slot_pooled(
                                                    versioned_tx,
                                                    tx.slot,
                                                    get_high_perf_clock(),
                                                );

                                            // Process transaction with backpressure control in EventProcessor
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
                                            error!(
                                                "Error converting ARPC transaction: {e:?}"
                                            );
                                        }
                                    }
                                }
                            }
                            Some(Err(error)) => {
                                error!("Stream error: {error:?}");
                                break;
                            }
                            None => break,
                        }
                    }
                    Some(update) = control_rx.next() => {
                        // Received subscription update, send new request to stream
                        if let Err(e) = subscribe_tx.send(update).await {
                            error!("Failed to send subscription update: {}", e);
                            break;
                        }
                        info!("Subscription updated successfully");
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

/// Convert ARPC SubscribeResponseTransaction to Solana VersionedTransaction
fn convert_arpc_to_versioned_transaction(
    arpc_tx: &SubscribeResponseTransaction,
) -> AnyResult<VersionedTransaction> {
    // Parse header - ARPC has flat structure
    let header = SolanaMessageHeader {
        num_required_signatures: arpc_tx.num_required_signatures as u8,
        num_readonly_signed_accounts: arpc_tx.num_readonly_signed_accounts as u8,
        num_readonly_unsigned_accounts: arpc_tx.num_readonly_unsigned_accounts as u8,
    };

    // Parse account keys
    let account_keys: Vec<Pubkey> = arpc_tx
        .account_keys
        .iter()
        .map(|key_bytes| {
            Pubkey::try_from(key_bytes.as_slice())
                .map_err(|e| anyhow::anyhow!("Invalid account key: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Parse recent blockhash
    let blockhash_array: [u8; 32] = arpc_tx
        .recent_blockhash
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid blockhash length"))?;
    let recent_blockhash = Hash::new_from_array(blockhash_array);

    // Parse instructions
    let instructions: Vec<SolanaCompiledInstruction> = arpc_tx
        .instructions
        .iter()
        .map(|ix| SolanaCompiledInstruction {
            program_id_index: ix.program_id_index as u8,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        })
        .collect();

    // Check if this is a V0 message (with address table lookups)
    let message = if !arpc_tx.address_table_lookups.is_empty() {
        // V0 message with address table lookups
        let address_table_lookups: Vec<SolanaMessageAddressTableLookup> = arpc_tx
            .address_table_lookups
            .iter()
            .map(|lookup| {
                let account_key = Pubkey::try_from(lookup.account_key.as_slice())
                    .map_err(|e| anyhow::anyhow!("Invalid lookup account key: {}", e))?;

                Ok(SolanaMessageAddressTableLookup {
                    account_key,
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        let v0_message = solana_sdk::message::v0::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
        };

        VersionedMessage::V0(v0_message)
    } else {
        // Legacy message
        let legacy_message = solana_sdk::message::legacy::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        };

        VersionedMessage::Legacy(legacy_message)
    };

    // Parse signatures
    let signatures: Vec<Signature> = arpc_tx
        .signatures
        .iter()
        .map(|sig_bytes| {
            Signature::try_from(sig_bytes.as_slice())
                .map_err(|e| anyhow::anyhow!("Invalid signature: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(VersionedTransaction { signatures, message })
}
