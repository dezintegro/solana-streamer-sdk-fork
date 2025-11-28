# Protocol Comparison: ARPC vs ShredStream vs Yellowstone

## Overview

This document compares the three protocol implementations in solana-streamer-sdk:
- **ShredStream**: Direct Solana node connection via UDP/QUIC
- **Yellowstone gRPC**: Triton's gRPC streaming protocol
- **ARPC**: Custom gRPC protocol with server-side filtering

## Module Structure Comparison

### ShredStream
```
src/streaming/shred/
├── connection.rs        # ShredStreamGrpc client
├── types.rs            # TransactionWithSlot, EntryWithSlot
├── pool.rs             # factory::create_transaction_with_slot_pooled
└── mod.rs              # Module exports
```

### Yellowstone
```
src/streaming/yellowstone/
├── connection.rs        # YellowstoneGrpcClient
├── types.rs            # TransactionPretty wrapper
├── pool.rs             # factory::create_transaction_pretty
└── mod.rs              # Module exports
```

### ARPC
```
src/streaming/arpc/
├── connection.rs        # ArpcGrpc client (same pattern!)
├── types.rs            # Re-exports shred::types::TransactionWithSlot
├── pool.rs             # Re-exports shred::pool::factory
└── mod.rs              # Module exports

src/streaming/arpc_stream.rs  # Subscription + conversion logic
```

**Key Difference**: ARPC re-exports from shred instead of creating new types.

## Client API Comparison

### Initialization

| Protocol    | API                                          |
|-------------|----------------------------------------------|
| ShredStream | `ShredStreamGrpc::new(endpoint).await?`     |
| Yellowstone | `YellowstoneGrpcClient::new(endpoint).await?`|
| ARPC        | `ArpcGrpc::new(endpoint).await?`            |

**Similarity**: All use identical constructor pattern.

### High-Throughput Mode

| Protocol    | API                                                    |
|-------------|--------------------------------------------------------|
| ShredStream | `ShredStreamGrpc::new_high_throughput(endpoint).await?`|
| Yellowstone | `YellowstoneGrpcClient::new_high_throughput(endpoint).await?` |
| ARPC        | `ArpcGrpc::new_high_throughput(endpoint).await?`      |

**Similarity**: All support high-throughput configuration.

### Custom Configuration

| Protocol    | API                                                         |
|-------------|-------------------------------------------------------------|
| ShredStream | `ShredStreamGrpc::new_with_config(endpoint, config).await?`|
| Yellowstone | `YellowstoneGrpcClient::new_with_config(endpoint, config).await?` |
| ARPC        | `ArpcGrpc::new_with_config(endpoint, config).await?`      |

**Similarity**: All use same StreamClientConfig.

## Subscription API Comparison

### ShredStream

```rust
client.shredstream_subscribe(
    protocols: Vec<Protocol>,
    bot_wallet: Option<Pubkey>,
    event_type_filter: Option<EventType>,
    callback: impl Fn(Box<dyn EventData>) + Send + Sync + 'static,
).await?
```

**Parameters**: 4
- protocols
- bot_wallet
- event_type_filter
- callback

### Yellowstone

```rust
client.yellowstone_subscribe(
    protocols: Vec<Protocol>,
    bot_wallet: Option<Pubkey>,
    event_type_filter: Option<EventType>,
    callback: impl Fn(Box<dyn EventData>) + Send + Sync + 'static,
).await?
```

**Parameters**: 4 (identical to ShredStream!)

### ARPC

```rust
client.arpc_subscribe(
    protocols: Vec<Protocol>,
    bot_wallet: Option<Pubkey>,
    event_type_filter: Option<EventType>,
    account_include: Vec<String>,      // ← ARPC-specific
    account_exclude: Vec<String>,      // ← ARPC-specific
    account_required: Vec<String>,     // ← ARPC-specific
    callback: impl Fn(Box<dyn EventData>) + Send + Sync + 'static,
).await?
```

**Parameters**: 7
- Same 4 as others
- Plus 3 ARPC-specific account filters

## Data Flow Comparison

### ShredStream Flow

```
UDP/QUIC Socket
      │
      ↓ Vec<Entry>
      │
┌─────┴──────────────────────────────┐
│ bincode::deserialize()              │
│  Entry → Vec<VersionedTransaction> │
└─────┬──────────────────────────────┘
      │
      ↓ for each tx
      │
┌─────┴──────────────────────────────┐
│ factory::create_transaction_with_  │
│        slot_pooled()                │
└─────┬──────────────────────────────┘
      │
      ↓ TransactionWithSlot
      │
┌─────┴──────────────────────────────┐
│ EventProcessor::process_shred_     │
│    transaction_with_metrics()      │
└─────┬──────────────────────────────┘
      │
      ↓ shred_queue.push()
```

**Conversion**: NO (already VersionedTransaction)

### Yellowstone Flow

```
gRPC Bidirectional Stream
      │
      ↓ SubscribeUpdate
      │
┌─────┴──────────────────────────────┐
│ Extract ConfirmedTransactionProto  │
└─────┬──────────────────────────────┘
      │
      ↓ ConfirmedTransactionProto
      │
┌─────┴──────────────────────────────┐
│ factory::create_transaction_pretty()│
│   • Convert Yellowstone → Solana    │
│   • Create TransactionPretty        │
└─────┬──────────────────────────────┘
      │
      ↓ TransactionPretty
      │
┌─────┴──────────────────────────────┐
│ EventProcessor::process_grpc_      │
│    transaction_with_metrics()      │
└─────┬──────────────────────────────┘
      │
      ↓ grpc_queue.push()
```

**Conversion**: YES (in factory::create_transaction_pretty)

### ARPC Flow

```
gRPC Bidirectional Stream
      │
      ↓ SubscribeResponse
      │
┌─────┴──────────────────────────────┐
│ Extract SubscribeResponseTransaction│
└─────┬──────────────────────────────┘
      │
      ↓ ARPC protobuf
      │
┌─────┴──────────────────────────────┐
│ convert_arpc_to_versioned_         │
│       transaction()                 │
│   • Convert ARPC → Solana           │
└─────┬──────────────────────────────┘
      │
      ↓ VersionedTransaction
      │
┌─────┴──────────────────────────────┐
│ factory::create_transaction_with_  │
│        slot_pooled()                │
└─────┬──────────────────────────────┘
      │
      ↓ TransactionWithSlot
      │
┌─────┴──────────────────────────────┐
│ EventProcessor::process_shred_     │
│    transaction_with_metrics()      │
└─────┬──────────────────────────────┘
      │
      ↓ shred_queue.push()
```

**Conversion**: YES (in arpc_stream.rs, before factory)

## Queue Usage Comparison

| Protocol    | Queue Type  | Event Type           | Processing Function                |
|-------------|-------------|----------------------|------------------------------------|
| ShredStream | shred_queue | TransactionWithSlot  | process_shred_transaction_with_metrics |
| Yellowstone | grpc_queue  | TransactionPretty    | process_grpc_transaction_with_metrics  |
| ARPC        | shred_queue | TransactionWithSlot  | process_shred_transaction_with_metrics |

**Key Insight**: ARPC uses the same queue and processing as ShredStream!

## Filtering Comparison

### Client-Side Filtering (ShredStream)

```rust
// Receive ALL transactions
for entry in entries {
    for tx in entry.transactions {
        // Filter after receiving
        if matches_protocol(tx, protocols) {
            process(tx);
        }
    }
}
```

**Pros**: Simple
**Cons**: Network waste (receive unwanted transactions)

### Server-Side Filtering (Yellowstone)

```rust
SubscribeRequest {
    accounts: {
        "client": SubscribeRequestFilterAccounts {
            account: vec!["Program111...".to_string()],
            ..Default::default()
        }
    },
    ..Default::default()
}
```

**Pros**: Less network traffic
**Cons**: Complex subscription setup

### Server-Side Filtering (ARPC)

```rust
SubscribeRequest {
    filters: Some(SubscribeRequestFilters {
        account_include: vec!["Program111...".to_string()],
        account_exclude: vec!["Spam222...".to_string()],
        account_required: vec!["Required333...".to_string()],
    }),
}
```

**Pros**:
- Less network traffic
- Simple, clear API
- Multiple filter types

**Cons**: Requires server support

## Protocol Message Structure

### ShredStream: Entry (bincode)

```rust
struct Entry {
    num_hashes: u64,
    hash: Hash,
    transactions: Vec<VersionedTransaction>,  // ← Already standard format
}
```

**Structure**: Nested (Entry → Vec<Transaction>)
**Format**: bincode (binary)
**Nesting Level**: 2

### Yellowstone: ConfirmedTransactionProto

```protobuf
message ConfirmedTransactionProto {
  Transaction transaction = 1;           // ← Nested
  TransactionStatusMeta meta = 2;        // ← Nested
}

message Transaction {
  repeated bytes signatures = 1;
  Message message = 2;                   // ← Nested
}

message Message {
  MessageHeader header = 1;              // ← Nested
  repeated bytes account_keys = 2;
  bytes recent_blockhash = 3;
  repeated CompiledInstruction instructions = 4;
}
```

**Structure**: Deeply nested (4+ levels)
**Format**: Protobuf
**Nesting Level**: 4

### ARPC: SubscribeResponseTransaction

```protobuf
message SubscribeResponseTransaction {
  uint64 slot = 1;                                    // ← Flat
  uint32 num_required_signatures = 2;                 // ← Flat
  uint32 num_readonly_signed_accounts = 3;            // ← Flat
  uint32 num_readonly_unsigned_accounts = 4;          // ← Flat
  bytes recent_blockhash = 5;                         // ← Flat
  repeated bytes signatures = 6;                      // ← Flat
  repeated bytes account_keys = 7;                    // ← Flat
  repeated CompiledInstruction instructions = 8;      // ← Flat
  repeated MessageAddressTableLookup address_table_lookups = 9;  // ← Flat
}
```

**Structure**: Flat (all fields at top level)
**Format**: Protobuf
**Nesting Level**: 1

## Conversion Complexity Comparison

### ShredStream: No Conversion

```rust
let tx = bincode::deserialize::<VersionedTransaction>(&data)?;
// ✓ One line, no conversion
```

**Lines of code**: 1
**Complexity**: O(1)

### Yellowstone: Built-in Conversion

```rust
pub fn create_transaction_pretty(
    tx: ConfirmedTransactionProto,
    slot: u64,
) -> AnyResult<PooledTransactionPretty> {
    // Extract nested transaction
    let transaction = tx.transaction
        .ok_or_else(|| anyhow::anyhow!("Missing transaction"))?;

    // Extract nested message
    let message = transaction.message
        .ok_or_else(|| anyhow::anyhow!("Missing message"))?;

    // Extract nested header
    let header = message.header
        .ok_or_else(|| anyhow::anyhow!("Missing header"))?;

    // Convert signatures
    let signatures = transaction.signatures.iter()...;

    // Convert message
    let msg = convert_message(message)?;

    // ... more conversion
}
```

**Lines of code**: ~100
**Complexity**: O(n) where n = nested structure depth

### ARPC: Manual Conversion

```rust
pub fn convert_arpc_to_versioned_transaction(
    arpc_tx: &SubscribeResponseTransaction,
) -> AnyResult<VersionedTransaction> {
    // 1. Create header (direct mapping)
    let header = SolanaMessageHeader {
        num_required_signatures: arpc_tx.num_required_signatures as u8,
        num_readonly_signed_accounts: arpc_tx.num_readonly_signed_accounts as u8,
        num_readonly_unsigned_accounts: arpc_tx.num_readonly_unsigned_accounts as u8,
    };

    // 2. Parse account keys (already flat)
    let account_keys: Vec<Pubkey> = arpc_tx.account_keys.iter()...;

    // 3. Parse blockhash (already flat)
    let recent_blockhash = Hash::new_from_array(...);

    // 4. Convert instructions (already flat)
    let instructions = arpc_tx.instructions.iter()...;

    // 5. Determine message type
    let message = if !arpc_tx.address_table_lookups.is_empty() {
        VersionedMessage::V0(...)
    } else {
        VersionedMessage::Legacy(...)
    };

    // 6. Parse signatures (already flat)
    let signatures = arpc_tx.signatures.iter()...;

    Ok(VersionedTransaction { signatures, message })
}
```

**Lines of code**: ~80
**Complexity**: O(n) where n = number of accounts + signatures

**Key Advantage**: No nested extraction, all fields accessible directly.

## Performance Comparison

### Memory Allocations per Transaction

| Protocol    | Allocations                                          | Count |
|-------------|------------------------------------------------------|-------|
| ShredStream | • VersionedTransaction (already exists)              | 1     |
|             | • TransactionWithSlot wrapper                        | 1     |
|             | **Total**                                            | **2** |
| Yellowstone | • ConfirmedTransactionProto (from protobuf)          | 1     |
|             | • Intermediate extraction structs                    | 3-4   |
|             | • VersionedTransaction (converted)                   | 1     |
|             | • TransactionPretty wrapper                          | 1     |
|             | **Total**                                            | **6-7** |
| ARPC        | • SubscribeResponseTransaction (from protobuf)       | 1     |
|             | • VersionedTransaction (converted)                   | 1     |
|             | • TransactionWithSlot wrapper                        | 1     |
|             | **Total**                                            | **3** |

### Latency Estimates

| Protocol    | Deserialization | Conversion | Total      |
|-------------|----------------|------------|------------|
| ShredStream | ~5-10 μs       | 0 μs       | ~5-10 μs   |
| Yellowstone | ~15-20 μs      | ~20-30 μs  | ~35-50 μs  |
| ARPC        | ~10-15 μs      | ~10-20 μs  | ~20-35 μs  |

**Note**: ARPC is faster than Yellowstone due to flat structure.

## Code Reuse Analysis

### Types Reuse

| Protocol    | Reuses shred types? | Creates new types? |
|-------------|--------------------|--------------------|
| ShredStream | N/A (original)     | Defines TransactionWithSlot |
| Yellowstone | NO                 | TransactionPretty  |
| ARPC        | **YES**            | None (re-exports)  |

### Pool Reuse

| Protocol    | Reuses shred pool? | Creates new factory? |
|-------------|-------------------|--------------------|
| ShredStream | N/A (original)    | Defines factory    |
| Yellowstone | NO                | create_transaction_pretty |
| ARPC        | **YES**           | None (re-exports)  |

### Queue Reuse

| Protocol    | Queue Used  | Creates new queue? |
|-------------|-------------|-------------------|
| ShredStream | shred_queue | Defines queue     |
| Yellowstone | grpc_queue  | YES               |
| ARPC        | shred_queue | **NO (reuses)**   |

**Winner**: ARPC maximizes code reuse!

## Feature Comparison

| Feature                  | ShredStream | Yellowstone | ARPC |
|--------------------------|-------------|-------------|------|
| Transaction streaming    | ✓           | ✓           | ✓    |
| Account updates          | ✗           | ✓           | ✗    |
| Server-side filtering    | ✗           | ✓           | ✓    |
| Batched transactions     | ✓ (Vec)     | ✗           | ✗    |
| Slot metadata            | ✓           | ✓           | ✓    |
| MetricsManager           | ✓           | ✓           | ✓    |
| Backpressure control     | ✓           | ✓           | ✓    |
| Protocol parsing         | ✓           | ✓           | ✓    |
| Event callbacks          | ✓           | ✓           | ✓    |
| Ping/pong keepalive      | ✗           | ✓           | ✗*   |

*Not implemented yet, but supported by protocol

## Network Protocol Comparison

| Aspect         | ShredStream     | Yellowstone        | ARPC              |
|----------------|-----------------|--------------------|-------------------|
| Transport      | UDP/QUIC        | gRPC (HTTP/2)      | gRPC (HTTP/2)     |
| Connection     | Connectionless  | Bidirectional      | Bidirectional     |
| Reliability    | Best-effort     | Reliable           | Reliable          |
| Ordering       | No guarantee    | Guaranteed         | Guaranteed        |
| Batching       | Yes (Vec<Entry>)| No                 | No                |
| Backpressure   | Application     | TCP + Application  | TCP + Application |

## Error Handling Comparison

### ShredStream

```rust
match bincode::deserialize::<Entry>(&data) {
    Ok(entry) => process(entry),
    Err(e) => {
        log::warn!("Deserialization error: {}", e);
        // Skip, continue
    }
}
```

**Strategy**: Skip malformed entries, continue stream

### Yellowstone

```rust
let transaction = tx.transaction
    .ok_or_else(|| anyhow::anyhow!("Missing transaction"))?;

// ← Propagates error, may stop stream
```

**Strategy**: Fail fast on malformed data

### ARPC

```rust
match convert_arpc_to_versioned_transaction(&arpc_tx) {
    Ok(tx) => process(tx),
    Err(e) => {
        log::warn!("Conversion error: {}", e);
        // Skip, continue stream
    }
}
```

**Strategy**: Skip malformed transactions, continue stream (like ShredStream)

## Implementation Complexity

### Lines of Code (approx.)

| Protocol    | connection.rs | types.rs | pool.rs | stream.rs | Total |
|-------------|--------------|----------|---------|-----------|-------|
| ShredStream | ~300         | ~100     | ~150    | ~200      | ~750  |
| Yellowstone | ~300         | ~150     | ~200    | ~250      | ~900  |
| ARPC        | ~300         | **~10**  | **~10** | ~200      | ~520  |

**Key Insight**: ARPC has ~30% less code due to re-exports!

## When to Use Each Protocol

### Use ShredStream When:
- ✓ Direct node connection available
- ✓ Need maximum throughput
- ✓ Can handle unreliable transport
- ✓ Want batched processing

### Use Yellowstone When:
- ✓ Need account updates
- ✓ Need transaction metadata (fees, logs)
- ✓ Server-side filtering required
- ✓ Managed infrastructure (Triton, Helius)

### Use ARPC When:
- ✓ Need server-side transaction filtering
- ✓ Want reliable gRPC connection
- ✓ Simple account-based filtering sufficient
- ✓ Prefer flat data structure
- ✓ Want minimal client code

## Conclusion

### Key Takeaways

1. **Architecture**: All three follow the same overall pattern (Connection → EventProcessor → Queue → Parser → Callback)

2. **Code Reuse**: ARPC maximizes reuse by using shred types and pools

3. **Conversion**:
   - ShredStream: None needed
   - Yellowstone: Complex (nested structure)
   - ARPC: Simple (flat structure)

4. **Performance**:
   - ShredStream: Fastest (no conversion)
   - ARPC: Fast (simple conversion)
   - Yellowstone: Slower (complex conversion)

5. **Features**:
   - ShredStream: Batching, direct connection
   - Yellowstone: Most features (accounts, metadata)
   - ARPC: Server filtering, simplicity

The ARPC implementation demonstrates excellent software engineering:
- **DRY**: Reuses existing code
- **KISS**: Simple, flat structure
- **SOLID**: Single responsibility (conversion isolated)
- **Performance**: Minimal overhead
- **Maintainability**: Less code to maintain
