use solana_sdk::transaction::VersionedTransaction;
use prost_types::Timestamp;

/// Transaction with slot and timestamp information from Shreder
#[derive(Debug, Clone, Default)]
pub struct ShrederTransaction {
    pub transaction: VersionedTransaction,
    pub slot: u64,
    pub recv_us: i64,
    pub created_at: Option<Timestamp>,
}

impl ShrederTransaction {
    /// Create new Shreder transaction
    pub fn new(
        transaction: VersionedTransaction,
        slot: u64,
        recv_us: i64,
        created_at: Option<Timestamp>,
    ) -> Self {
        Self {
            transaction,
            slot,
            recv_us,
            created_at,
        }
    }
}
