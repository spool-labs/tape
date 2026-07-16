use rpc_solana::{RpcConfig, SolanaRpc};
use rpc::{EncodedConfirmedTransactionWithStatusMeta, Rpc, RpcError, UiConfirmedBlock};
use tape_crypto::tx::Txid;

#[cfg(feature = "metrics")]
use std::sync::Arc;

/// RPC client for Tapedrive on-chain program queries.
///
/// Wraps an RPC implementation and provides convenient methods for
/// interacting with Tapedrive-specific accounts and operations.
///
/// This client is generic over `R: Rpc`, enabling:
/// - Production use with `RpcClient<SolanaRpc>` (retry/failover)
/// - Testing with `RpcClient<TestRpc>` (local test validator)
///
/// # Example
///
/// ```ignore
/// // Production
/// let client = RpcClient::new(config)?;
///
/// // Testing
/// let client = RpcClient::from_rpc(TestRpc::new(&validator));
/// ```
pub struct RpcClient<R: Rpc> {
    rpc: R,
    #[cfg(feature = "metrics")]
    pub metrics: Option<Arc<crate::metrics::ClientMetrics>>,
}

impl<R: Rpc> RpcClient<R> {
    /// Creates a new RpcClient from any Rpc implementation
    ///
    /// # Arguments
    /// * `rpc` - The RPC implementation to use
    pub fn from_rpc(rpc: R) -> Self {
        Self {
            rpc,
            #[cfg(feature = "metrics")]
            metrics: None,
        }
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
    /// specifically wrapped by RpcClient.
    pub fn rpc(&self) -> &R {
        &self.rpc
    }

    // ========================================================================
    // Delegated RPC methods
    // ========================================================================

    /// Get the current slot.
    pub async fn get_slot(&self) -> Result<u64, RpcError> {
        self.rpc.get_slot().await
    }

    /// Get the most recently finalized slot.
    pub async fn get_finalized_slot(&self) -> Result<u64, RpcError> {
        self.rpc.get_finalized_slot().await
    }

    /// Get block by slot number.
    pub async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        self.rpc.get_block(slot).await
    }

    /// Get the lowest slot the node still has a confirmed block for.
    pub async fn get_first_available_block(&self) -> Result<u64, RpcError> {
        self.rpc.get_first_available_block().await
    }

    /// Get a confirmed transaction by signature.
    pub async fn get_transaction(
        &self,
        txid: &Txid,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, RpcError> {
        self.rpc.get_transaction(txid).await
    }
}

// ============================================================================
// Production-specific constructors (SolanaRpc)
// ============================================================================

impl RpcClient<SolanaRpc> {
    /// Creates a new RpcClient with the given configuration
    ///
    /// This is the primary constructor for production use. It creates
    /// a SolanaRpc with retry and failover capabilities.
    ///
    /// # Arguments
    /// * `config` - RPC configuration including endpoints, commitment, and retry settings
    ///
    /// # Errors
    /// Returns an error if the RPC client cannot be initialized
    pub fn new(config: RpcConfig) -> Result<Self, RpcError> {
        Ok(Self {
            rpc: SolanaRpc::new(config)?,
            #[cfg(feature = "metrics")]
            metrics: None,
        })
    }

    /// Creates a new RpcClient with metrics enabled
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
            rpc: SolanaRpc::new(config)?,
            metrics: Some(crate::metrics::ClientMetrics::new_with_global_registry()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = RpcConfig::default();
        let client = RpcClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_rpc_access() {
        let config = RpcConfig::default();
        let client = RpcClient::new(config).unwrap();
        let _rpc = client.rpc();
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_client_without_metrics() {
        let config = RpcConfig::default();
        let client = RpcClient::new(config).unwrap();
        assert!(client.metrics().is_none());
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_client_with_metrics() {
        let config = RpcConfig::default();
        let client = RpcClient::new_with_metrics(config).unwrap();
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

        let client = RpcClient::new(config).unwrap().with_metrics(metrics.clone());
        assert!(client.metrics().is_some());

        // Verify it's the same metrics instance
        let client_metrics = client.metrics().unwrap();
        assert!(Arc::ptr_eq(client_metrics, &metrics));
    }
}
