use anyhow::Result;
use solana_streamer_sdk::streaming::ArpcGrpc;
use solana_streamer_sdk::streaming::arpc::TransactionFilter;
use solana_streamer_sdk::streaming::event_parser::Protocol;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use solana_streamer_sdk::streaming::event_parser::common::EventType;
use solana_streamer_sdk::streaming::event_parser::common::filter::EventTypeFilter;

const PUMPSWAP_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAYDIUM_CPMM_PROGRAM_ID: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

/// Simple example demonstrating dynamic subscription updates with ARPC
///
/// This example:
/// 1. Subscribes to PumpSwap program transactions
/// 2. Waits for first event
/// 3. Updates subscription to RaydiumCpmm program transactions
/// 4. Continues monitoring
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Get ARPC endpoint from environment or use default
    let endpoint = std::env::var("ARPC_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:20202".to_string());

    println!("=== ARPC Dynamic Subscription Example ===");
    println!("Connecting to ARPC endpoint: {}", endpoint);
    println!();

    // Create ARPC client
    let client = Arc::new(ArpcGrpc::new(endpoint).await?);

    let event_counter = Arc::new(AtomicU64::new(0));
    let first_event_received = Arc::new(AtomicBool::new(false));

    let counter_clone = event_counter.clone();
    let first_event_flag = first_event_received.clone();

    let callback = move |event: Box<dyn solana_streamer_sdk::streaming::event_parser::UnifiedEvent>| {
        let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "Event #{}: {} - Signature: {:.8}...",
            count,
            event.event_type(),
            event.signature()
        );

        // Mark that we received first event
        first_event_flag.store(true, Ordering::Relaxed);
    };

    // === Phase 1: Subscribe to PumpSwap ===
    println!("Phase 1: Subscribing to PumpSwap program ({})...", PUMPSWAP_PROGRAM_ID);
    let trade_event_filter = EventTypeFilter {
        include: vec![
            EventType::PumpSwapBuy,
            EventType::PumpSwapSell,
            EventType::RaydiumCpmmSwapBaseInput,
            EventType::RaydiumCpmmSwapBaseOutput,
        ],
    };

    // Create transaction filter for PumpSwap
    let transaction_filter = TransactionFilter {
        account_include: vec![PUMPSWAP_PROGRAM_ID.to_string()],
        account_exclude: vec![],
        account_required: vec![],
    };

    client
        .arpc_subscribe(
            vec![Protocol::PumpSwap, Protocol::RaydiumCpmm],
            None,
            Some(trade_event_filter),
            vec![transaction_filter],
            callback,
        )
        .await?;

    println!("✓ Subscribed to PumpSwap transactions");
    println!("Waiting for first event (max 30 seconds)...");
    println!();

    // Wait for first event or timeout
    let mut waited = 0;
    while !first_event_received.load(Ordering::Relaxed) && waited < 30 {
        sleep(Duration::from_secs(1)).await;
        waited += 1;

        if waited % 5 == 0 {
            println!("  Still waiting... ({} seconds)", waited);
        }
    }

    let phase1_count = event_counter.load(Ordering::Relaxed);

    if phase1_count == 0 {
        println!("⚠ No events received in Phase 1 (this is normal if PumpSwap is quiet)");
    } else {
        println!("✓ Phase 1 complete: {} events received", phase1_count);
    }
    println!();

    // === Phase 2: Update to RaydiumCpmm ===
    println!("Phase 2: Updating subscription to RaydiumCpmm program ({})...", RAYDIUM_CPMM_PROGRAM_ID);

    // Create transaction filter for RaydiumCpmm
    let transaction_filter = TransactionFilter {
        account_include: vec![RAYDIUM_CPMM_PROGRAM_ID.to_string()],
        account_exclude: vec![],
        account_required: vec![],
    };

    client
        .update_subscription(vec![transaction_filter])
        .await?;

    println!("✓ Subscription updated to RaydiumCpmm transactions");
    println!("Monitoring for 15 seconds...");
    println!();

    // Monitor for 15 seconds
    for i in 1..=15 {
        sleep(Duration::from_secs(1)).await;

        if i % 5 == 0 {
            let current_count = event_counter.load(Ordering::Relaxed);
            let phase2_events = current_count - phase1_count;
            println!("  {} seconds: {} new events", i, phase2_events);
        }
    }

    let final_count = event_counter.load(Ordering::Relaxed);
    let phase2_count = final_count - phase1_count;

    println!();
    println!("✓ Phase 2 complete: {} events received", phase2_count);
    println!();

    // === Summary ===
    println!("=== Summary ===");
    println!("Phase 1 (PumpSwap):     {} events", phase1_count);
    println!("Phase 2 (RaydiumCpmm):  {} events", phase2_count);
    println!("Total:                  {} events", final_count);
    println!();

    // Stop subscription
    println!("Stopping subscription...");
    client.stop().await;
    println!("✓ Subscription stopped");

    Ok(())
}
