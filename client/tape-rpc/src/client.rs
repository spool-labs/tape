use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::failover::EndpointFailover;
use crate::retry::ExponentialBackoff;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Solana RPC client with retry and failover capabilities
///
/// Wraps the Solana RpcClient with automatic retry logic and endpoint failover.
/// All operations automatically retry on transient errors and failover to backup
/// endpoints when necessary.
pub struct TapeRpcClient {
    config: RpcConfig,
    failover: Arc<RwLock<EndpointFailover>>,
    client: Arc<RwLock<RpcClient>>,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<crate::metrics::RpcMetrics>>,
}

impl TapeRpcClient {
    /// Creates a new TapeRpcClient with the given configuration
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid (e.g., no endpoints)
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

    /// Attempt to failover to the next endpoint
    async fn try_failover(&self) -> Result<(), RpcError> {
        let mut failover = self.failover.write().await;

        #[cfg(feature = "metrics")]
        let old_endpoint = failover.current_endpoint().to_string();

        let new_endpoint = failover.next_endpoint()?;
        let new_endpoint_str = new_endpoint.to_string();
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

    /// Handle error and determine if retry should continue
    async fn handle_error(&self, method: &str, err: RpcError, backoff: &mut ExponentialBackoff) -> Result<(), RpcError> {
        #[cfg(feature = "metrics")]
        tracing::warn!(
            category = err.category(),
            retriable = err.is_retriable(),
            "RPC operation failed"
        );

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_error(method, err.category());
        }

        // If not retriable, return immediately
        if !err.is_retriable() {
            return Err(err);
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_retry(method, err.category());
        }

        // Try failover if appropriate
        if err.should_failover() {
            if let Err(e) = self.try_failover().await {
                #[cfg(feature = "metrics")]
                tracing::warn!(error = ?e, "Failover failed");

                // If failover fails with exhausted endpoints, return that error
                if matches!(e, RpcError::AllEndpointsFailed { .. }) {
                    return Err(e);
                }
            }
        }

        // Check if we can retry
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

    /// Handle timeout and determine if retry should continue
    async fn handle_timeout(&self, method: &str, backoff: &mut ExponentialBackoff) -> Result<(), RpcError> {
        let timeout_err = RpcError::Timeout(self.config.timeout);

        #[cfg(feature = "metrics")]
        tracing::warn!(
            timeout = ?self.config.timeout,
            "RPC operation timed out"
        );

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_error(method, "timeout");
            metrics.record_retry(method, "timeout");
        }

        // Try failover on timeout
        if let Err(e) = self.try_failover().await {
            if matches!(e, RpcError::AllEndpointsFailed { .. }) {
                return Err(e);
            }
        }

        // Check if we can retry
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

    /// Reset failover state on success
    async fn on_success(&self) {
        let mut failover = self.failover.write().await;
        failover.reset();
    }

    // ========================================================================
    // Account Operations
    // ========================================================================

    /// Fetch a single account's data
    pub async fn get_account(&self, pubkey: &Pubkey) -> Result<Account, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkey = *pubkey;
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_account(&pubkey),
                )
                .await
            };

            match result {
                Ok(Ok(account)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getAccount", "success", timer.elapsed_secs());
                    }

                    return Ok(account);
                }
                Ok(Err(e)) => {
                    let rpc_err = if e.to_string().contains("AccountNotFound") {
                        RpcError::AccountNotFound(pubkey)
                    } else {
                        RpcError::from(e)
                    };
                    self.handle_error("getAccount", rpc_err, &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getAccount", &mut backoff).await?;
                }
            }
        }
    }

    /// Fetch multiple accounts in a single request
    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkeys = pubkeys.to_vec();
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_multiple_accounts(&pubkeys),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getMultipleAccounts", "success", timer.elapsed_secs());
                    }

                    return Ok(accounts);
                }
                Ok(Err(e)) => {
                    self.handle_error("getMultipleAccounts", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getMultipleAccounts", &mut backoff).await?;
                }
            }
        }
    }

    /// Fetch program accounts with filters
    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Pubkey, Account)>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let program_id = *program_id;
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let config_clone = config.clone();
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_program_accounts_with_config(&program_id, config_clone),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getProgramAccounts", "success", timer.elapsed_secs());
                    }

                    return Ok(accounts);
                }
                Ok(Err(e)) => {
                    self.handle_error("getProgramAccounts", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getProgramAccounts", &mut backoff).await?;
                }
            }
        }
    }

    // ========================================================================
    // Slot/Block Operations
    // ========================================================================

    /// Get the current slot
    pub async fn get_slot(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_slot()).await
            };

            match result {
                Ok(Ok(slot)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getSlot", "success", timer.elapsed_secs());
                    }

                    return Ok(slot);
                }
                Ok(Err(e)) => {
                    self.handle_error("getSlot", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSlot", &mut backoff).await?;
                }
            }
        }
    }

    /// Get the latest blockhash
    pub async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_latest_blockhash()).await
            };

            match result {
                Ok(Ok(hash)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getLatestBlockhash", "success", timer.elapsed_secs());
                    }

                    return Ok(hash);
                }
                Ok(Err(e)) => {
                    self.handle_error("getLatestBlockhash", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getLatestBlockhash", &mut backoff).await?;
                }
            }
        }
    }

    /// Get block by slot number
    ///
    /// Returns the confirmed block at the given slot with full transaction details.
    /// This is used for future block processing implementations.
    pub async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = ExponentialBackoff::new(&self.config.retry);
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
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlock", "success", timer.elapsed_secs());
                    }

                    return Ok(block);
                }
                Ok(Err(e)) => {
                    self.handle_error("getBlock", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlock", &mut backoff).await?;
                }
            }
        }
    }

    /// Get the block height
    pub async fn get_block_height(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = ExponentialBackoff::new(&self.config.retry);

        loop {
            let result = {
                let client = self.client.read().await;
                tokio::time::timeout(self.config.timeout, client.get_block_height()).await
            };

            match result {
                Ok(Ok(height)) => {
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlockHeight", "success", timer.elapsed_secs());
                    }

                    return Ok(height);
                }
                Ok(Err(e)) => {
                    self.handle_error("getBlockHeight", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlockHeight", &mut backoff).await?;
                }
            }
        }
    }

    // ========================================================================
    // Transaction Operations
    // ========================================================================

    /// Send a transaction without waiting for confirmation
    pub async fn send_transaction(&self, transaction: &Transaction) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let transaction = transaction.clone();
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

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
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("sendTransaction", "success", timer.elapsed_secs());
                    }

                    return Ok(sig);
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    let rpc_err = if err_str.contains("blockhash not found")
                        || err_str.contains("Blockhash not found")
                    {
                        RpcError::BlockhashExpired
                    } else {
                        RpcError::from(e)
                    };
                    self.handle_error("sendTransaction", rpc_err, &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendTransaction", &mut backoff).await?;
                }
            }
        }
    }

    /// Send a transaction and wait for confirmation
    ///
    /// Uses polling to check transaction status until it's confirmed.
    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let transaction = transaction.clone();
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

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
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("sendAndConfirmTransaction", "success", timer.elapsed_secs());
                    }

                    return Ok(sig);
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    let rpc_err = if err_str.contains("blockhash not found")
                        || err_str.contains("Blockhash not found")
                    {
                        RpcError::BlockhashExpired
                    } else if err_str.contains("Transaction simulation failed") {
                        RpcError::Transaction(err_str)
                    } else {
                        RpcError::from(e)
                    };
                    self.handle_error("sendAndConfirmTransaction", rpc_err, &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendAndConfirmTransaction", &mut backoff).await?;
                }
            }
        }
    }

    /// Check the status of a transaction signature
    ///
    /// Returns `Ok(Some(Ok(())))` if confirmed successfully,
    /// `Ok(Some(Err(e)))` if confirmed with error,
    /// `Ok(None)` if not yet confirmed.
    pub async fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<Result<(), solana_sdk::transaction::TransactionError>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let signature = *signature;
        let mut backoff = ExponentialBackoff::new(&self.config.retry);

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
                    self.on_success().await;

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getSignatureStatus", "success", timer.elapsed_secs());
                    }

                    return Ok(status);
                }
                Ok(Err(e)) => {
                    self.handle_error("getSignatureStatus", RpcError::from(e), &mut backoff).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSignatureStatus", &mut backoff).await?;
                }
            }
        }
    }

    /// Get the current commitment configuration
    pub fn commitment(&self) -> solana_sdk::commitment_config::CommitmentLevel {
        self.config.commitment
    }

    /// Get a reference to the configuration
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = RpcConfig::default();
        let client = TapeRpcClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_client_creation_no_endpoints() {
        let config = RpcConfig {
            endpoints: vec![],
            ..Default::default()
        };
        let client = TapeRpcClient::new(config);
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_commitment_config() {
        let config = RpcConfig {
            commitment: solana_sdk::commitment_config::CommitmentLevel::Finalized,
            ..Default::default()
        };
        let client = TapeRpcClient::new(config).unwrap();
        assert_eq!(
            client.commitment(),
            solana_sdk::commitment_config::CommitmentLevel::Finalized
        );
    }
}
