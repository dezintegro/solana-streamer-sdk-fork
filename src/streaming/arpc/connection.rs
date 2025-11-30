use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use futures::channel::mpsc;
use futures::SinkExt;
use anyhow::anyhow;

use crate::common::AnyResult;
use crate::protos::arpc::{arpc_service_client::ArpcServiceClient, SubscribeRequest, SubscribeRequestFilterTransactions};
use crate::streaming::common::{
    MetricsManager, PerformanceMetrics, StreamClientConfig, SubscriptionHandle,
};

/// ARPC gRPC client for streaming transactions
#[derive(Clone)]
pub struct ArpcGrpc {
    pub arpc_client: Arc<ArpcServiceClient<Channel>>,
    pub config: StreamClientConfig,
    pub metrics: Arc<RwLock<PerformanceMetrics>>,
    pub metrics_manager: MetricsManager,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
    // Dynamic subscription management fields
    pub active_subscription: Arc<AtomicBool>,
    pub control_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<SubscribeRequest>>>>,
    pub current_request: Arc<tokio::sync::RwLock<Option<SubscribeRequest>>>,
}

impl ArpcGrpc {
    /// Create a new client with default configuration
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    /// Create a new client with custom configuration
    pub async fn new_with_config(endpoint: String, config: StreamClientConfig) -> AnyResult<Self> {
        let arpc_client = ArpcServiceClient::connect(endpoint.clone()).await?;
        let metrics = Arc::new(RwLock::new(PerformanceMetrics::new()));

        let metrics_manager = MetricsManager::new(config.enable_metrics, "ARPC".to_string());

        Ok(Self {
            arpc_client: Arc::new(arpc_client),
            config,
            metrics: metrics.clone(),
            metrics_manager,
            subscription_handle: Arc::new(Mutex::new(None)),
            active_subscription: Arc::new(AtomicBool::new(false)),
            control_tx: Arc::new(tokio::sync::Mutex::new(None)),
            current_request: Arc::new(tokio::sync::RwLock::new(None)),
        })
    }

    /// Create a new client with high-throughput configuration.
    ///
    /// Optimized for high-concurrency scenarios where throughput is prioritized over latency.
    pub async fn new_high_throughput(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::high_throughput()).await
    }

    /// Create a new client with low-latency configuration.
    ///
    /// Optimized for real-time scenarios where latency is prioritized over throughput.
    pub async fn new_low_latency(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::low_latency()).await
    }

    /// Get current configuration
    pub fn get_config(&self) -> &StreamClientConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: StreamClientConfig) {
        self.config = config;
    }

    /// Get performance metrics
    pub fn get_metrics(&self) -> PerformanceMetrics {
        self.metrics_manager.get_metrics()
    }

    /// Enable or disable performance monitoring
    pub fn set_enable_metrics(&mut self, enabled: bool) {
        self.config.enable_metrics = enabled;
    }

    /// Print performance metrics
    pub fn print_metrics(&self) {
        self.metrics_manager.print_metrics();
    }

    /// Start automatic performance monitoring
    pub async fn start_auto_metrics_monitoring(&self) {
        self.metrics_manager.start_auto_monitoring().await;
    }

    /// Stop current subscription
    pub async fn stop(&self) {
        let mut handle_guard = self.subscription_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.stop();
        }
        *self.control_tx.lock().await = None;
        *self.current_request.write().await = None;
        self.active_subscription.store(false, Ordering::Release);
    }

    /// Update subscription filters at runtime without reconnection
    ///
    /// # Parameters
    /// * `account_include` - Accounts to include in the filter
    /// * `account_exclude` - Accounts to exclude from the filter
    /// * `account_required` - Accounts that are required in transactions
    ///
    /// # Returns
    /// Returns `AnyResult<()>` on success, error on failure
    pub async fn update_subscription(
        &self,
        account_include: Vec<String>,
        account_exclude: Vec<String>,
        account_required: Vec<String>,
    ) -> AnyResult<()> {
        // Get control sender (clone to avoid holding lock during await)
        let mut control_sender = {
            let control_guard = self.control_tx.lock().await;

            if !self.active_subscription.load(Ordering::Acquire) {
                return Err(anyhow!("No active subscription to update"));
            }

            control_guard
                .as_ref()
                .ok_or_else(|| anyhow!("No active subscription to update"))?
                .clone()
        };

        // Clone current request
        let mut request = self
            .current_request
            .read()
            .await
            .as_ref()
            .ok_or_else(|| anyhow!("No active subscription"))?
            .clone();

        // Update transaction filters
        let filter = SubscribeRequestFilterTransactions {
            account_include,
            account_exclude,
            account_required,
        };

        let mut filters = HashMap::new();
        filters.insert("transactions".to_string(), filter);

        request.transactions = filters;

        // Send update through control channel
        control_sender
            .send(request.clone())
            .await
            .map_err(|e| anyhow!("Failed to send update: {}", e))?;

        // Save updated request
        *self.current_request.write().await = Some(request);

        Ok(())
    }
}
