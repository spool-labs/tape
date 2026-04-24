//! Production Solana RPC client with retry and failover capabilities.

use crate::config::RpcConfig;
use crate::failover::EndpointFailover;
use async_trait::async_trait;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcTransactionConfig};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiConfirmedBlock, UiTransactionEncoding,
};
use std::sync::Arc;
use rpc::{Rpc, RpcError};
use tape_crypto::address::Address;
use tape_crypto::tx::Txid;
use tokio::sync::RwLock;

/// Production Solana RPC client with retry and failover capabilities.
///
/// Wraps the Solana RpcClient with automatic retry logic and endpoint failover.
/// All operations automatically retry on transient errors and failover to backup
/// endpoints when necessary.
///
/// # Example
///
/// ```ignore
/// use rpc_solana::{SolanaRpc, RpcConfig};
/// use rpc::Rpc;
///
/// let config = RpcConfig {
///     endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
///     ..Default::default()
/// };
/// let rpc = SolanaRpc::new(config)?;
///
/// let slot = rpc.get_slot().await?;
/// ```
pub struct SolanaRpc {
    config: RpcConfig,
    failover: Arc<RwLock<EndpointFailover>>,
    client: Arc<RwLock<RpcClient>>,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<crate::metrics::RpcMetrics>>,
}

impl SolanaRpc {
    /// Creates a new SolanaRpc with the given configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid (e.g., no endpoints).
    pub fn new(config: RpcConfig) -> Result<Self, RpcError> {
        if config.endpoints.is_empty() {
            return Err(RpcError::Internal("No endpoints configured".to_string()));
        }

        let commitment = CommitmentConfig {
            commitment: config.commitment,
        };

        let first_endpoint = config.endpoints[0].clone();
        let client = RpcClient::new_with_commitment(first_endpoint, commitment);

        let failover = EndpointFailover::new(
            config.endpoints.clone(),
            config.retry.max_endpoint_attempts,
        );

        #[cfg(feature = "metrics")]
        let metrics = {
            if let Some(registry) = tape_metrics::MetricsRegistry::get() {
                let metrics = Arc::new(crate::metrics::RpcMetrics::new(
                    registry.prometheus_registry(),
                ));
                metrics.set_endpoints_configured(config.endpoints.len());
                metrics.set_current_endpoint(0);
                Some(metrics)
            } else {
                None
            }
        };

        Ok(Self {
            config,
            failover: Arc::new(RwLock::new(failover)),
            client: Arc::new(RwLock::new(client)),
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    /// Attempt to failover to the next endpoint.
    async fn try_failover(&self) -> Result<(), RpcError> {
        let mut failover = self.failover.write().await;

        #[cfg(feature = "metrics")]
        let old_endpoint = failover.current_endpoint().to_string();

        let new_endpoint = failover.next_endpoint()?;
        let new_endpoint_str = new_endpoint.to_string();
        #[cfg(feature = "metrics")]
        let current_index = failover.current_index();

        #[cfg(feature = "metrics")]
        tracing::info!(endpoint = %new_endpoint_str, "Switching RPC endpoint");

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_failover(&old_endpoint, "endpoint_error");
            metrics.set_current_endpoint(current_index);
        }

        let commitment = CommitmentConfig {
            commitment: self.config.commitment,
        };

        let new_client = RpcClient::new_with_commitment(new_endpoint_str, commitment);

        let mut client = self.client.write().await;
        *client = new_client;

        Ok(())
    }

    /// Handle error and determine if retry should continue.
    ///
    /// Failover is best-effort: if all endpoints have been tried, we still
    /// retry on the current endpoint using backoff. Backoff is the sole
    /// authority on when to stop retrying.
    async fn handle_error(
        &self,
        _method: &str,
        err: RpcError,
        backoff: &mut tape_retry::Backoff,
    ) -> Result<(), RpcError> {
        #[cfg(feature = "metrics")]
        tracing::warn!(
            category = err.category(),
            retriable = err.is_retriable(),
            error = %err,
            "RPC operation failed"
        );

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_error(_method, err.category());
        }

        // If not retriable, return immediately
        if !err.is_retriable() {
            return Err(err);
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_retry(_method, err.category());
        }

        // Try failover if appropriate (best-effort, if exhausted, we still
        // retry on the current endpoint via backoff below)
        if err.should_failover() {
            let _ = self.try_failover().await;
        }

        // Backoff controls when to stop retrying
        if let Some(delay) = backoff.next_delay() {
            #[cfg(feature = "metrics")]
            tracing::debug!(
                attempt = backoff.attempt(),
                delay_ms = delay.as_millis(),
                "Retrying after error"
            );

            tokio::time::sleep(delay).await;
            Ok(())
        } else {
            // Max retries exceeded
            Err(err)
        }
    }

    /// Handle timeout and determine if retry should continue.
    ///
    /// Failover is best-effort: if all endpoints have been tried, we still
    /// retry on the current endpoint using backoff.
    async fn handle_timeout(
        &self,
        _method: &str,
        backoff: &mut tape_retry::Backoff,
    ) -> Result<(), RpcError> {
        let timeout_err = RpcError::Timeout(self.config.timeout);

        #[cfg(feature = "metrics")]
        tracing::warn!(
            timeout = ?self.config.timeout,
            "RPC operation timed out"
        );

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_error(_method, "timeout");
            metrics.record_retry(_method, "timeout");
        }

        // Try failover on timeout (best-effort)
        let _ = self.try_failover().await;

        // Backoff controls when to stop retrying
        if let Some(delay) = backoff.next_delay() {
            #[cfg(feature = "metrics")]
            tracing::debug!(
                attempt = backoff.attempt(),
                delay_ms = delay.as_millis(),
                "Retrying after timeout"
            );

            tokio::time::sleep(delay).await;
            Ok(())
        } else {
            Err(timeout_err)
        }
    }

    /// Reset failover state for a fresh operation.
    async fn reset_failover(&self) {
        let mut failover = self.failover.write().await;
        failover.reset();
    }

    /// Convert a ClientError to RpcError.
    fn convert_error(err: ClientError, pubkey: Option<Address>) -> RpcError {
        let err_str = flatten_error(&err);

        // Check for specific error types
        if err_str.contains("AccountNotFound") {
            if let Some(pk) = pubkey {
                return RpcError::AccountNotFound(pk);
            }
        }

        if err_str.contains("blockhash not found") || err_str.contains("Blockhash not found") {
            return RpcError::BlockhashExpired;
        }

        if err_str.contains("Transaction simulation failed") {
            return RpcError::Transaction(err_str);
        }

        // Default to Request error with the error message
        RpcError::Request(err_str)
    }

    fn normalize_get_block_error(error: RpcError) -> RpcError {
        if let RpcError::Request(message) = error {
            if Self::is_block_not_available_message(&message) {
                return RpcError::BlockNotAvailable;
            }

            return RpcError::Request(message);
        }

        error
    }

    fn is_block_not_available_message(message: &str) -> bool {
        message.contains("invalid type: null") && message.contains("UiConfirmedBlock")
    }

}

// ============================================================================
// Rpc trait implementation
// ============================================================================

#[async_trait]
impl Rpc for SolanaRpc {
    fn commitment(&self) -> solana_sdk::commitment_config::CommitmentLevel {
        self.config.commitment
    }

    async fn get_slot(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_slot()).await
            };

            match result {
                Ok(Ok(slot)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getSlot", "success", timer.elapsed_secs());
                    }

                    return Ok(slot);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getSlot", rpc_err, &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSlot", &mut backoff).await?;
                }
            }
        }
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_latest_blockhash()).await
            };

            match result {
                Ok(Ok(hash)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getLatestBlockhash", "success", timer.elapsed_secs());
                    }

                    return Ok(hash);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getLatestBlockhash", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getLatestBlockhash", &mut backoff)
                        .await?;
                }
            }
        }
    }

    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;
        let commitment = self.config.commitment;

        loop {
            let config = RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Json),
                transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
                rewards: Some(true),
                commitment: Some(CommitmentConfig { commitment }),
                max_supported_transaction_version: Some(0),
            };

            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_block_with_config(slot, config),
                )
                .await
            };

            match result {
                Ok(Ok(block)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlock", "success", timer.elapsed_secs());
                    }

                    return Ok(block);
                }
                Ok(Err(e)) => {
                    let rpc_error = Self::normalize_get_block_error(Self::convert_error(e, None));
                    self.handle_error("getBlock", rpc_error, &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlock", &mut backoff).await?;
                }
            }
        }
    }

    async fn get_transaction(
        &self,
        txid: &Txid,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let txid = *txid;
        let signature: Signature = txid.into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;
        let commitment = self.config.commitment;

        loop {
            let config = RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(CommitmentConfig { commitment }),
                max_supported_transaction_version: Some(0),
            };

            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_transaction_with_config(&signature, config),
                )
                .await
            };

            match result {
                Ok(Ok(transaction)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getTransaction",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(transaction);
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    let rpc_err = if err_str.contains("not found")
                        || err_str.contains("Transaction version not supported")
                    {
                        RpcError::TransactionNotFound(txid)
                    } else {
                        Self::convert_error(e, None)
                    };
                    self.handle_error("getTransaction", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getTransaction", &mut backoff).await?;
                }
            }
        }
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_block_height()).await
            };

            match result {
                Ok(Ok(height)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlockHeight", "success", timer.elapsed_secs());
                    }

                    return Ok(height);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getBlockHeight", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlockHeight", &mut backoff).await?;
                }
            }
        }
    }

    async fn get_account(&self, pubkey: &Address) -> Result<Account, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkey = *pubkey;
        let solana_pubkey: Pubkey = pubkey.into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_account(&solana_pubkey)).await
            };

            match result {
                Ok(Ok(account)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getAccount", "success", timer.elapsed_secs());
                    }

                    return Ok(account);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, Some(pubkey));
                    self.handle_error("getAccount", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getAccount", &mut backoff).await?;
                }
            }
        }
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Address],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkeys = pubkeys.to_vec();
        let solana_pubkeys: Vec<Pubkey> = pubkeys.iter().copied().map(Into::into).collect();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_multiple_accounts(&solana_pubkeys),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getMultipleAccounts",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(accounts);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getMultipleAccounts", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getMultipleAccounts", &mut backoff)
                        .await?;
                }
            }
        }
    }

    async fn get_program_accounts(
        &self,
        program_id: &Address,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Address, Account)>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let program_id = *program_id;
        let solana_program_id: Pubkey = program_id.into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let config_clone = config.clone();
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_program_accounts_with_config(&solana_program_id, config_clone),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getProgramAccounts",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(accounts
                        .into_iter()
                        .map(|(address, account)| (address.into(), account))
                        .collect());
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getProgramAccounts", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getProgramAccounts", &mut backoff)
                        .await?;
                }
            }
        }
    }

    async fn send_transaction(&self, transaction: &Transaction) -> Result<Txid, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let transaction = transaction.clone();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.send_transaction(&transaction),
                )
                .await
            };

            match result {
                Ok(Ok(sig)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("sendTransaction", "success", timer.elapsed_secs());
                    }

                    return Ok(sig.into());
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("sendTransaction", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendTransaction", &mut backoff).await?;
                }
            }
        }
    }

    async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Txid, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let transaction = transaction.clone();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.send_and_confirm_transaction(&transaction),
                )
                .await
            };

            match result {
                Ok(Ok(sig)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "sendAndConfirmTransaction",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(sig.into());
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("sendAndConfirmTransaction", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendAndConfirmTransaction", &mut backoff)
                        .await?;
                }
            }
        }
    }

    async fn get_signature_status(
        &self,
        txid: &Txid,
    ) -> Result<Option<Result<(), solana_sdk::transaction::TransactionError>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let signature: Signature = (*txid).into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        self.reset_failover().await;

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_signature_status(&signature),
                )
                .await
            };

            match result {
                Ok(Ok(status)) => {
                    self.reset_failover().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getSignatureStatus",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(status);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getSignatureStatus", rpc_err, &mut backoff)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSignatureStatus", &mut backoff)
                        .await?;
                }
            }
        }
    }
}

fn flatten_error(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        out.push_str(": ");
        out.push_str(&inner.to_string());
        source = inner.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = RpcConfig::default();
        let client = SolanaRpc::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_creation_no_endpoints() {
        let config = RpcConfig {
            endpoints: vec![],
            ..Default::default()
        };
        let client = SolanaRpc::new(config);
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_commitment_config() {
        let config = RpcConfig {
            commitment: solana_sdk::commitment_config::CommitmentLevel::Finalized,
            ..Default::default()
        };
        let client = SolanaRpc::new(config).unwrap();
        assert_eq!(
            client.commitment(),
            solana_sdk::commitment_config::CommitmentLevel::Finalized
        );
    }

    #[test]
    fn test_normalize_get_block_error_maps_null_block_to_block_not_available() {
        let error = RpcError::Request(
            "RPC request failed: invalid type: null, expected struct UiConfirmedBlock"
                .to_string(),
        );

        let normalized = SolanaRpc::normalize_get_block_error(error);

        assert!(matches!(normalized, RpcError::BlockNotAvailable));
    }

    #[test]
    fn test_normalize_get_block_error_leaves_other_request_errors_unchanged() {
        let error = RpcError::Request("connection reset".to_string());

        let normalized = SolanaRpc::normalize_get_block_error(error);

        match normalized {
            RpcError::Request(message) => assert_eq!(message, "connection reset"),
            other => panic!("expected request error, got {other:?}"),
        }
    }
}
