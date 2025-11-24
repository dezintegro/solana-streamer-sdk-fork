use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::ops::DerefMut;
use solana_sdk::transaction::VersionedTransaction;
use prost_types::Timestamp;

use super::ShrederTransaction;

/// ShrederTransaction object pool
pub struct ShrederTransactionPool {
    pool: Arc<Mutex<VecDeque<Box<ShrederTransaction>>>>,
    max_size: usize,
}

impl ShrederTransactionPool {
    pub fn new(initial_size: usize, max_size: usize) -> Self {
        let mut pool = VecDeque::with_capacity(initial_size);

        // Pre-allocate objects
        for _ in 0..initial_size {
            pool.push_back(Box::new(ShrederTransaction::default()));
        }

        Self {
            pool: Arc::new(Mutex::new(pool)),
            max_size,
        }
    }

    pub fn acquire(&self) -> PooledShrederTransaction {
        let mut pool = self.pool.lock().unwrap();
        let transaction = match pool.pop_front() {
            Some(reused) => reused,
            None => Box::new(ShrederTransaction::default()),
        };

        PooledShrederTransaction {
            transaction,
            pool: Arc::clone(&self.pool),
            max_size: self.max_size,
        }
    }
}

/// ShrederTransaction with automatic return to pool
pub struct PooledShrederTransaction {
    transaction: Box<ShrederTransaction>,
    pool: Arc<Mutex<VecDeque<Box<ShrederTransaction>>>>,
    max_size: usize,
}

impl PooledShrederTransaction {
    /// Reset from raw data
    pub fn reset_from_data(
        &mut self,
        transaction: VersionedTransaction,
        slot: u64,
        recv_us: i64,
        created_at: Option<Timestamp>,
    ) {
        self.transaction.transaction = transaction;
        self.transaction.slot = slot;
        self.transaction.recv_us = recv_us;
        self.transaction.created_at = created_at;
    }

    /// Create ShrederTransaction using optimized factory method (move data instead of clone)
    pub fn into_shreder_transaction(mut self) -> ShrederTransaction {
        // Move data instead of clone to avoid extra memory allocation
        std::mem::replace(self.deref_mut(), ShrederTransaction::default())
    }
}

impl Drop for PooledShrederTransaction {
    fn drop(&mut self) {
        let mut pool = self.pool.lock().unwrap();
        if pool.len() < self.max_size {
            // Clear sensitive data
            self.transaction.slot = 0;
            self.transaction.recv_us = 0;
            self.transaction.created_at = None;
            // Reset transaction to default to clear sensitive data
            self.transaction.transaction = VersionedTransaction::default();
            pool.push_back(std::mem::take(&mut self.transaction));
        }
    }
}

impl std::ops::Deref for PooledShrederTransaction {
    type Target = ShrederTransaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl std::ops::DerefMut for PooledShrederTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}

/// Shreder pool manager
pub struct ShrederPoolManager {
    transaction_pool: ShrederTransactionPool,
}

impl ShrederPoolManager {
    pub fn new() -> Self {
        Self {
            transaction_pool: ShrederTransactionPool::new(
                5000,  // Initial size - Shreder typically has high event volume
                15000, // Max size
            ),
        }
    }

    pub fn get_transaction_pool(&self) -> &ShrederTransactionPool {
        &self.transaction_pool
    }

    /// Create optimized ShrederTransaction
    pub fn create_shreder_transaction_optimized(
        &self,
        transaction: VersionedTransaction,
        slot: u64,
        recv_us: i64,
        created_at: Option<Timestamp>,
    ) -> ShrederTransaction {
        let mut pooled_tx = self.transaction_pool.acquire();
        pooled_tx.reset_from_data(transaction, slot, recv_us, created_at);
        pooled_tx.into_shreder_transaction()
    }
}

impl Default for ShrederPoolManager {
    fn default() -> Self {
        Self::new()
    }
}

// Global Shreder pool manager instance
lazy_static::lazy_static! {
    pub static ref GLOBAL_SHREDER_POOL_MANAGER: ShrederPoolManager = ShrederPoolManager::new();
}

/// Convenient global factory functions
pub mod factory {
    use super::*;

    /// Create ShrederTransaction using object pool (recommended for high-performance scenarios)
    pub fn create_shreder_transaction_pooled(
        transaction: VersionedTransaction,
        slot: u64,
        recv_us: i64,
        created_at: Option<Timestamp>,
    ) -> ShrederTransaction {
        GLOBAL_SHREDER_POOL_MANAGER.create_shreder_transaction_optimized(
            transaction,
            slot,
            recv_us,
            created_at,
        )
    }
}
