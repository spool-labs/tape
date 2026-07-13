//! Production Solana RPC client with retry and failover capabilities.

use crate::config::RpcConfig;
#[cfg(feature = "metrics")]
use crate::selector::EndpointStrategy;
use crate::selector::{EndpointCursor, EndpointSelector};
use async_trait::async_trait;
use solana_account::Account;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig, RpcTransactionConfig};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::{Transaction, TransactionError};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock,
    UiTransactionEncoding,
};
#[cfg(feature = "metrics")]
use std::sync::Arc;
use rpc::{Rpc, RpcError};
use tape_crypto::address::Address;
use tape_crypto::tx::Txid;
use tokio::sync::Mutex;

const BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: Some(UiTransactionEncoding::Json),
    transaction_details: Some(TransactionDetails::Full),
    rewards: Some(false),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    }),
    max_supported_transaction_version: Some(0),
};

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
    /// Endpoint, retry, and strategy settings this client was built with
    config: RpcConfig,
    /// Decides which endpoint each operation runs on
    selector: Mutex<EndpointSelector>,
    /// One client per configured endpoint, so switching endpoints never
    /// rebuilds a client or drops its connection pool
    clients: Vec<RpcClient>,
    /// Prometheus counters, absent when no registry is installed
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

        let clients = config
            .endpoints
            .iter()
            .map(|endpoint| RpcClient::new_with_commitment(endpoint.clone(), commitment))
            .collect();

        let selector = EndpointSelector::new(
            config.endpoints.clone(),
            config.strategy,
            config.retry.max_endpoint_attempts,
            config.retry.endpoint_cooldown,
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
            selector: Mutex::new(selector),
            clients,
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    /// Pick the endpoint a fresh operation starts on
    async fn start_operation(&self) -> EndpointCursor {
        let (cursor, _is_moved) = self.selector.lock().await.start_operation();

        #[cfg(feature = "metrics")]
        if _is_moved {
            // Round-robin moves every operation; only a recovered primary is
            // worth a log line.
            if matches!(self.config.strategy, EndpointStrategy::PreferPrimary) {
                tracing::info!(
                    endpoint = %redact_url_query(&self.config.endpoints[cursor.index()]),
                    "Restoring RPC endpoint"
                );
            }
            if let Some(metrics) = &self.metrics {
                metrics.set_current_endpoint(cursor.index());
            }
        }

        cursor
    }

    /// Rotate a failing operation to the next untried endpoint
    ///
    /// Best-effort: when the rotation is exhausted the operation keeps
    /// retrying where it is via backoff.
    async fn fail_over(&self, cursor: &mut EndpointCursor) {
        let mut selector = self.selector.lock().await;

        #[cfg(feature = "metrics")]
        let old_endpoint = redact_url_query(selector.endpoint(cursor.index()));

        if selector.fail_over(cursor).is_err() {
            return;
        }

        // Endpoint urls carry an api key in the query string, and the metrics
        // endpoint is served publicly.
        #[cfg(feature = "metrics")]
        {
            tracing::info!(
                endpoint = %redact_url_query(selector.endpoint(cursor.index())),
                "Switching RPC endpoint"
            );
            if let Some(metrics) = &self.metrics {
                metrics.record_failover(&old_endpoint, "endpoint_error");
                metrics.set_current_endpoint(cursor.index());
            }
        }
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
        cursor: &mut EndpointCursor,
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
            self.fail_over(cursor).await;
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
        cursor: &mut EndpointCursor,
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
        self.fail_over(cursor).await;

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

        if let Some(tx_error) = err.get_transaction_error() {
            let tx_error = tx_error.to_string();
            if err_str.contains(&tx_error) {
                return RpcError::Transaction(err_str);
            }
            return RpcError::Transaction(format!("{tx_error}; {err_str}"));
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

/// Send a transaction and poll its signature status at a short fixed cadence.
/// The library confirm loop polls every 500ms, which adds up to half a slot of
/// pure sleep per transaction on top of confirmation itself.
async fn send_and_poll(
    client: &RpcClient,
    transaction: &Transaction,
    commitment: CommitmentLevel,
) -> Result<Signature, ClientError> {
    const CONFIRM_POLL_MS: u64 = 200;

    let commitment = CommitmentConfig { commitment };
    let signature = client
        .send_transaction_with_config(
            transaction,
            RpcSendTransactionConfig {
                // The write paths construct well-formed transactions and read
                // failures from signature status, so the preflight simulation
                // only adds a round-trip.
                skip_preflight: true,
                ..RpcSendTransactionConfig::default()
            },
        )
        .await?;

    loop {
        if let Some(status) = client
            .get_signature_status_with_commitment(&signature, commitment)
            .await?
        {
            status?;
            return Ok(signature);
        }
        tokio::time::sleep(std::time::Duration::from_millis(CONFIRM_POLL_MS)).await;
    }
}

// ============================================================================
// Rpc trait implementation
// ============================================================================

#[async_trait]
impl Rpc for SolanaRpc {
    fn commitment(&self) -> CommitmentLevel {
        self.config.commitment
    }

    async fn get_slot(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(self.config.timeout, client.get_slot()).await
            };

            match result {
                Ok(Ok(slot)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getSlot", "success", timer.elapsed_secs());
                    }

                    return Ok(slot);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getSlot", rpc_err, &mut backoff, &mut cursor).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSlot", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_finalized_slot(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        let commitment = CommitmentConfig::finalized();

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_slot_with_commitment(commitment),
                )
                .await
            };

            match result {
                Ok(Ok(slot)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .record_request("getSlot:finalized", "success", timer.elapsed_secs());
                    }

                    return Ok(slot);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getSlot:finalized", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSlot:finalized", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_first_available_block(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_first_available_block(),
                )
                .await
            };

            match result {
                Ok(Ok(slot)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getFirstAvailableBlock",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(slot);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getFirstAvailableBlock", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getFirstAvailableBlock", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(self.config.timeout, client.get_latest_blockhash()).await
            };

            match result {
                Ok(Ok(hash)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getLatestBlockhash", "success", timer.elapsed_secs());
                    }

                    return Ok(hash);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getLatestBlockhash", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getLatestBlockhash", &mut backoff, &mut cursor)
                        .await?;
                }
            }
        }
    }

    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_block_with_config(slot, BLOCK_CONFIG),
                )
                .await
            };

            match result {
                Ok(Ok(block)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlock", "success", timer.elapsed_secs());
                    }

                    return Ok(block);
                }
                Ok(Err(e)) => {
                    let rpc_error = Self::normalize_get_block_error(Self::convert_error(e, None));
                    self.handle_error("getBlock", rpc_error, &mut backoff, &mut cursor).await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlock", &mut backoff, &mut cursor).await?;
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
        let mut cursor = self.start_operation().await;
        let commitment = self.config.commitment;

        loop {
            let config = RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(CommitmentConfig { commitment }),
                max_supported_transaction_version: Some(0),
            };

            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_transaction_with_config(&signature, config),
                )
                .await
            };

            match result {
                Ok(Ok(transaction)) => {
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
                    self.handle_error("getTransaction", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getTransaction", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(self.config.timeout, client.get_block_height()).await
            };

            match result {
                Ok(Ok(height)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getBlockHeight", "success", timer.elapsed_secs());
                    }

                    return Ok(height);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getBlockHeight", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getBlockHeight", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_account(&self, pubkey: &Address) -> Result<Account, RpcError> {
        self.get_account_with_commitment(pubkey, self.config.commitment)
            .await
    }

    async fn get_account_with_commitment(
        &self,
        pubkey: &Address,
        commitment: CommitmentLevel,
    ) -> Result<Account, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkey = *pubkey;
        let solana_pubkey: Pubkey = pubkey.into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;
        let commitment = CommitmentConfig { commitment };

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_account_with_commitment(&solana_pubkey, commitment),
                )
                .await
            };

            match result {
                Ok(Ok(response)) => {
                    let Some(account) = response.value else {
                        return Err(RpcError::AccountNotFound(pubkey));
                    };

                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("getAccount", "success", timer.elapsed_secs());
                    }

                    return Ok(account);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, Some(pubkey));
                    self.handle_error("getAccount", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getAccount", &mut backoff, &mut cursor).await?;
                }
            }
        }
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Address],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        self.get_multiple_accounts_with_commitment(pubkeys, self.config.commitment)
            .await
    }

    async fn get_multiple_accounts_with_commitment(
        &self,
        pubkeys: &[Address],
        commitment: CommitmentLevel,
    ) -> Result<Vec<Option<Account>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let pubkeys = pubkeys.to_vec();
        let solana_pubkeys: Vec<Pubkey> = pubkeys.iter().copied().map(Into::into).collect();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;
        let commitment = CommitmentConfig { commitment };

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_multiple_accounts_with_commitment(&solana_pubkeys, commitment),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(
                            "getMultipleAccounts",
                            "success",
                            timer.elapsed_secs(),
                        );
                    }

                    return Ok(accounts.value);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getMultipleAccounts", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getMultipleAccounts", &mut backoff, &mut cursor)
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
        let mut cursor = self.start_operation().await;

        loop {
            let config_clone = config.clone();
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_program_ui_accounts_with_config(&solana_program_id, config_clone),
                )
                .await
            };

            match result {
                Ok(Ok(accounts)) => {
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
                        .map(|(address, account)| {
                            account
                                .to_account()
                                .map(|account| (address.into(), account))
                                .ok_or_else(|| {
                                    RpcError::Deserialization(
                                        "program account was not binary-decodable".to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?);
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("getProgramAccounts", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getProgramAccounts", &mut backoff, &mut cursor)
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
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.send_transaction(&transaction),
                )
                .await
            };

            match result {
                Ok(Ok(sig)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request("sendTransaction", "success", timer.elapsed_secs());
                    }

                    return Ok(sig.into());
                }
                Ok(Err(e)) => {
                    let rpc_err = Self::convert_error(e, None);
                    self.handle_error("sendTransaction", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendTransaction", &mut backoff, &mut cursor).await?;
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
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    send_and_poll(client, &transaction, self.config.commitment),
                )
                .await
            };

            match result {
                Ok(Ok(sig)) => {
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
                    self.handle_error("sendAndConfirmTransaction", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("sendAndConfirmTransaction", &mut backoff, &mut cursor)
                        .await?;
                }
            }
        }
    }

    async fn get_signature_status(
        &self,
        txid: &Txid,
    ) -> Result<Option<Result<(), TransactionError>>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let signature: Signature = (*txid).into();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation().await;

        loop {
            let result = {
                let client = &self.clients[cursor.index()];
                tokio::time::timeout(
                    self.config.timeout,
                    client.get_signature_status(&signature),
                )
                .await
            };

            match result {
                Ok(Ok(status)) => {
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
                    self.handle_error("getSignatureStatus", rpc_err, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout("getSignatureStatus", &mut backoff, &mut cursor)
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
    redact_url_query(&out)
}

/// Strip query strings from URLs embedded in error text; they carry
/// credentials (`?api=<key>`) and end up in logs.
fn redact_url_query(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;
    while let Some(pos) = rest.find('?') {
        let (head, tail) = rest.split_at(pos);
        out.push_str(head);
        let in_url = head
            .rsplit(|c: char| c.is_whitespace() || c == '(')
            .next()
            .is_some_and(|token| token.starts_with("http://") || token.starts_with("https://"));
        if in_url {
            let end = tail
                .find([')', ']', '"', '\'', ' ', '\t', '\n'])
                .unwrap_or(tail.len());
            out.push_str("?<redacted>");
            rest = &tail[end..];
        } else {
            out.push('?');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_client::client_error::ClientErrorKind;
    use solana_instruction_error::InstructionError;
    use solana_transaction::TransactionError;

    #[test]
    fn test_client_creation() {
        let config = RpcConfig::default();
        let client = SolanaRpc::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn redacts_url_query_in_error_text() {
        let msg = "HTTP status server error (502 Bad Gateway) for url (http://143.198.31.76:8899/?api=18a91636db43f5602b706be879b3ad531c32b8248871ee50)";
        let redacted = redact_url_query(msg);
        assert_eq!(
            redacted,
            "HTTP status server error (502 Bad Gateway) for url (http://143.198.31.76:8899/?<redacted>)"
        );

        let plain = "was the slot skipped? maybe";
        assert_eq!(redact_url_query(plain), plain);
    }

    // a bare endpoint url is redacted before it reaches a log or a metric label
    #[test]
    fn redacts_bare_endpoint_url() {
        let endpoint = "https://devnet.helius-rpc.com/?api-key=3109787a-400a-407d";
        assert_eq!(
            redact_url_query(endpoint),
            "https://devnet.helius-rpc.com/?<redacted>"
        );

        let keyless = "http://127.0.0.1:8899";
        assert_eq!(redact_url_query(keyless), keyless);
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
            commitment: CommitmentLevel::Finalized,
            ..Default::default()
        };
        let client = SolanaRpc::new(config).unwrap();
        assert_eq!(client.commitment(), CommitmentLevel::Finalized);
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

    #[test]
    fn test_convert_error_maps_structured_transaction_error() {
        let tx_error = TransactionError::InstructionError(
            1,
            InstructionError::Custom(0x12),
        );
        let error = ClientError::from(ClientErrorKind::TransactionError(tx_error));

        let converted = SolanaRpc::convert_error(error, None);

        match converted {
            RpcError::Transaction(message) => {
                assert!(message.contains("Error processing Instruction 1"));
                assert!(message.contains("custom program error: 0x12"));
            }
            other => panic!("expected transaction error, got {other:?}"),
        }
    }

    #[test]
    fn test_convert_error_keeps_blockhash_expired_specific() {
        let error = ClientError::from(ClientErrorKind::TransactionError(
            TransactionError::BlockhashNotFound,
        ));

        let converted = SolanaRpc::convert_error(error, None);

        assert!(matches!(converted, RpcError::BlockhashExpired));
    }
}
