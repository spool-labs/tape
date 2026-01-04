use solana_transaction_status::UiConfirmedBlock;
use tape_rpc::{RpcConfig, RpcError, TapeRpcClient};

#[cfg(feature = "metrics")]
use std::sync::Arc;

/// High-level client for Tape v2 programs
///
/// Wraps the TapeRpcClient and provides convenient methods for
/// interacting with Tape-specific accounts and operations.
pub struct TapeClient {
    rpc: TapeRpcClient,
    #[cfg(feature = "metrics")]
    pub(crate) metrics: Option<Arc<crate::metrics::ClientMetrics>>,
}

impl TapeClient {
    /// Creates a new TapeClient with the given configuration
    ///
    /// # Arguments
    /// * `config` - RPC configuration including endpoints, commitment, and retry settings
    ///
    /// # Errors
    /// Returns an error if the RPC client cannot be initialized
    pub fn new(config: RpcConfig) -> Result<Self, RpcError> {
        Ok(Self {
            rpc: TapeRpcClient::new(config)?,
            #[cfg(feature = "metrics")]
            metrics: None,
        })
    }

    /// Creates a new TapeClient with metrics enabled
    ///
    /// # Arguments
    /// * `config` - RPC configuration including endpoints, commitment, and retry settings
    ///
    /// # Errors
    /// Returns an error if the RPC client cannot be initialized
    ///
    /// # Note
    /// This method is only available when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub fn new_with_metrics(config: RpcConfig) -> Result<Self, RpcError> {
        Ok(Self {
            rpc: TapeRpcClient::new(config)?,
            metrics: Some(crate::metrics::ClientMetrics::new_with_global_registry()),
        })
    }

    /// Set custom metrics for this client
    ///
    /// # Arguments
    /// * `metrics` - The metrics instance to use
    ///
    /// # Note
    /// This method is only available when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::ClientMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get a reference to the client's metrics, if enabled
    ///
    /// # Note
    /// This method is only available when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub fn metrics(&self) -> Option<&Arc<crate::metrics::ClientMetrics>> {
        self.metrics.as_ref()
    }

    /// Access the underlying RPC client for custom operations
    ///
    /// This allows direct access to low-level RPC methods that aren't
    /// specifically wrapped by TapeClient.
    pub fn rpc(&self) -> &TapeRpcClient {
        &self.rpc
    }

    // ========================================================================
    // Delegated RPC methods
    // ========================================================================

    /// Get the current slot.
    pub async fn get_slot(&self) -> Result<u64, RpcError> {
        self.rpc.get_slot().await
    }

    /// Get block by slot number.
    pub async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        self.rpc.get_block(slot).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = RpcConfig::default();
        let client = TapeClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_rpc_access() {
        let config = RpcConfig::default();
        let client = TapeClient::new(config).unwrap();
        let _rpc = client.rpc();
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_client_without_metrics() {
        let config = RpcConfig::default();
        let client = TapeClient::new(config).unwrap();
        assert!(client.metrics().is_none());
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_client_with_metrics() {
        let config = RpcConfig::default();
        let client = TapeClient::new_with_metrics(config).unwrap();
        assert!(client.metrics().is_some());
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_client_with_custom_metrics() {
        use crate::metrics::ClientMetrics;
        use std::sync::Arc;
        use tape_metrics::Registry;

        let config = RpcConfig::default();
        let metrics_registry = Registry::new();
        let metrics = Arc::new(ClientMetrics::new(&metrics_registry));

        let client = TapeClient::new(config).unwrap().with_metrics(metrics.clone());
        assert!(client.metrics().is_some());

        // Verify it's the same metrics instance
        let client_metrics = client.metrics().unwrap();
        assert!(Arc::ptr_eq(client_metrics, &metrics));
    }

}
