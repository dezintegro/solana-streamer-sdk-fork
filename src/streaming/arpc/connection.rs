use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::common::AnyResult;
use crate::protos::arpc::arpc_service_client::ArpcServiceClient;
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
    }
}
