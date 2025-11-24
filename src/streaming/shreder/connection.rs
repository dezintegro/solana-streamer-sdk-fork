use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::common::AnyResult;
use crate::protos::shreder::shreder_service_client::ShrederServiceClient;
use crate::streaming::common::{
    MetricsManager, PerformanceMetrics, StreamClientConfig, SubscriptionHandle,
};

/// Shreder gRPC client
#[derive(Clone)]
pub struct ShrederGrpc {
    pub shreder_client: Arc<ShrederServiceClient<Channel>>,
    pub config: StreamClientConfig,
    pub metrics: Arc<RwLock<PerformanceMetrics>>,
    pub metrics_manager: MetricsManager,
    pub subscription_handle: Arc<Mutex<Option<SubscriptionHandle>>>,
}

impl ShrederGrpc {
    /// Create client with default configuration
    pub async fn new(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::default()).await
    }

    /// Create client with custom configuration
    pub async fn new_with_config(endpoint: String, config: StreamClientConfig) -> AnyResult<Self> {
        let shreder_client = ShrederServiceClient::connect(endpoint.clone()).await?;
        let metrics = Arc::new(RwLock::new(PerformanceMetrics::new()));

        let metrics_manager = MetricsManager::new(config.enable_metrics, "Shreder".to_string());

        Ok(Self {
            shreder_client: Arc::new(shreder_client),
            config,
            metrics: metrics.clone(),
            metrics_manager,
            subscription_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Creates a new ShrederClient with high-throughput configuration.
    ///
    /// This is a convenience method that creates a client optimized for high-concurrency scenarios
    /// where throughput is prioritized over latency. See `StreamClientConfig::high_throughput()`
    /// for detailed configuration information.
    pub async fn new_high_throughput(endpoint: String) -> AnyResult<Self> {
        Self::new_with_config(endpoint, StreamClientConfig::high_throughput()).await
    }

    /// Creates a new ShrederClient with low-latency configuration.
    ///
    /// This is a convenience method that creates a client optimized for real-time scenarios
    /// where latency is prioritized over throughput. See `StreamClientConfig::low_latency()`
    /// for detailed configuration information.
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

    /// Start automatic performance monitoring task
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
