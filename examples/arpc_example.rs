use solana_streamer_sdk::streaming::ArpcGrpc;
use solana_streamer_sdk::streaming::event_parser::{Protocol, UnifiedEvent};
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
        .unwrap_or_else(|_| "http://localhost:10000".to_string());

    log::info!("Connecting to ARPC endpoint: {}", endpoint);

    // Create ARPC client
    let client = ArpcGrpc::new(endpoint).await?;

    // Counter for received transactions
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Account filters - replace with actual accounts you want to monitor
    let account_include = vec![
        // Example: Pump.fun program ID
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string(),
    ];

    log::info!("Starting ARPC subscription with filters:");
    log::info!("  Account include: {:?}", account_include);

    // Subscribe to transactions
    client
        .arpc_subscribe(
            vec![Protocol::PumpFun], // Monitor Pump.fun protocol
            None,                     // No bot wallet filter
            None,                     // No event type filter
            account_include,          // Include transactions with these accounts
            vec![],                   // No account exclude
            vec![],                   // No account required
            move |event| {
                let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;

                log::info!("Received event #{}: {:?}", count, event.event_type());

                // Print event details
                match event.event_type() {
                    solana_streamer_sdk::streaming::event_parser::common::types::EventType::PumpFun => {
                        log::info!("  Signature: {}", event.signature());
                        log::info!("  Slot: {:?}", event.slot());
                    }
                    _ => {
                        log::info!("  Event: {:?}", event);
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
