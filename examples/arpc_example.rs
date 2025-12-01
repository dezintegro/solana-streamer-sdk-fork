use solana_streamer_sdk::streaming::ArpcGrpc;
use solana_streamer_sdk::streaming::arpc::TransactionFilter;
use solana_streamer_sdk::streaming::event_parser::Protocol;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // ARPC endpoint - replace with actual endpoint
    let endpoint = std::env::var("ARPC_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:20202".to_string());

    log::info!("Connecting to ARPC endpoint: {}", endpoint);

    // Create ARPC client
    let client = ArpcGrpc::new(endpoint).await?;

    // Counter for received transactions
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Account filters - replace with actual accounts you want to monitor
    let account_include = vec![
        // Example: PumpSwap program ID
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(),
    ];

    log::info!("Starting ARPC subscription with filters:");
    log::info!("  Account include: {:?}", account_include);

    // Create transaction filter (similar to grpc_example.rs)
    let transaction_filter = TransactionFilter {
        account_include: account_include.clone(),
        account_exclude: vec![],
        account_required: vec![],
    };

    // Subscribe to transactions
    client
        .arpc_subscribe(
            vec![Protocol::PumpSwap], // Monitor Pump.fun protocol
            None,                     // No bot wallet filter
            None,                     // No event type filter
            vec![transaction_filter], // Transaction filters
            move |event| {
                let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;

                log::info!("Received event #{}: {}", count, event.event_type());
                log::info!("  Signature: {}", event.signature());
                log::info!("  Slot: {:?}", event.slot());
                log::info!("  Event: {:?}", event);

                // Print event details based on type
                use solana_streamer_sdk::streaming::event_parser::common::types::EventType;
                match event.event_type() {
                    EventType::PumpFunCreateToken => {
                        log::info!("  Type: PumpFun Token Creation");
                    }
                    EventType::PumpFunBuy => {
                        log::info!("  Type: PumpFun Buy");
                    }
                    EventType::PumpFunSell => {
                        log::info!("  Type: PumpFun Sell");
                    }
                    EventType::PumpFunMigrate => {
                        log::info!("  Type: PumpFun Migration");
                    }
                    _ => {
                        log::info!("  Type: Other event");
                    }
                }
            },
        )
        .await?;

    log::info!("Subscription started successfully!");
    log::info!("Press Ctrl+C to stop...");

    // Keep running until interrupted
    tokio::signal::ctrl_c().await?;

    log::info!("Shutting down...");
    log::info!("Total transactions received: {}", counter.load(Ordering::Relaxed));

    // Stop subscription
    client.stop().await;

    Ok(())
}
