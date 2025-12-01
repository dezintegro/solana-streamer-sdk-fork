use std::collections::HashMap;
use crate::protos::arpc::SubscribeRequestFilterTransactions;

// Re-export TransactionWithSlot from shred module to avoid duplication
pub use crate::streaming::shred::types::TransactionWithSlot;

/// Transaction filter for ARPC subscriptions
#[derive(Debug, Clone, Default)]
pub struct TransactionFilter {
    pub account_include: Vec<String>,
    pub account_exclude: Vec<String>,
    pub account_required: Vec<String>,
}

impl TransactionFilter {
    /// Create a new empty transaction filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a filter with included accounts
    pub fn with_include(mut self, accounts: Vec<String>) -> Self {
        self.account_include = accounts;
        self
    }

    /// Create a filter with excluded accounts
    pub fn with_exclude(mut self, accounts: Vec<String>) -> Self {
        self.account_exclude = accounts;
        self
    }

    /// Create a filter with required accounts
    pub fn with_required(mut self, accounts: Vec<String>) -> Self {
        self.account_required = accounts;
        self
    }
}

impl From<TransactionFilter> for SubscribeRequestFilterTransactions {
    fn from(filter: TransactionFilter) -> Self {
        SubscribeRequestFilterTransactions {
            account_include: filter.account_include,
            account_exclude: filter.account_exclude,
            account_required: filter.account_required,
        }
    }
}

pub type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
