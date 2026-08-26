//! Production Solana RPC client with retry and failover capabilities.

use crate::config::RpcConfig;
use crate::selector::{EndpointCursor, EndpointSelector};
use async_trait::async_trait;
use solana_account::Account;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcTransactionConfig};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::{Transaction, TransactionError};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails,
    UiTransactionEncoding,
};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use rpc::{Rpc, RpcError, SimulationResult};
use tape_crypto::address::Address;
use tape_blocks::wire::Block;
use tape_crypto::tx::Txid;

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
    clients: Vec<Arc<RpcClient>>,
    /// Used by getBlock, which owns its request so the body is deserialised
    /// once. Shared across endpoints, since reqwest pools per host.
    http: reqwest::Client,
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
            .map(|endpoint| Arc::new(RpcClient::new_with_commitment(endpoint.clone(), commitment)))
            .collect();

        let selector = EndpointSelector::new(
            config.endpoints.len(),
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
            // Compression is already on through solana-rpc-client's reqwest
            // features, and Accepts::default() enables whatever is compiled in.
            // Declared here anyway so this path does not depend on another
            // crate's feature list to keep decompressing getBlock.
            http: reqwest::Client::new(),
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    /// Pick the endpoint a fresh operation starts on
    fn start_operation(&self) -> EndpointCursor {
        let (cursor, _is_restored) = self
            .selector
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .start_operation();

        // Endpoint urls carry an api key in the query string, and the metrics
        // endpoint is served publicly.
        #[cfg(feature = "metrics")]
        if _is_restored {
            tracing::info!(
                endpoint = %redact_url_query(&self.config.endpoints[cursor.index()]),
                "Restoring RPC endpoint"
            );
            if let Some(metrics) = &self.metrics {
                metrics.set_current_endpoint(cursor.index());
            }
        }

        cursor
    }

    /// Rotate a failing operation to the next endpoint
    ///
    /// Best-effort: when the rotation is exhausted the operation keeps
    /// retrying where it is via backoff.
    fn fail_over(&self, cursor: &mut EndpointCursor) {
        #[cfg(feature = "metrics")]
        let old_index = cursor.index();

        let is_rotated = self
            .selector
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fail_over(cursor);
        if !is_rotated {
        }

        // Endpoint urls carry an api key in the query string, and the metrics
        // endpoint is served publicly.
        #[cfg(feature = "metrics")]
        {
            tracing::info!(
                endpoint = %redact_url_query(&self.config.endpoints[cursor.index()]),
                "Switching RPC endpoint"
            );
            if let Some(metrics) = &self.metrics {
                metrics.record_failover(
                    &redact_url_query(&self.config.endpoints[old_index]),
                    "endpoint_error",
                );
                metrics.set_current_endpoint(cursor.index());
            }
        }
    }

    /// Drive one RPC operation: pick an endpoint, run the call with a
    /// timeout, and retry with backoff and failover until it succeeds or the
    /// retry budget runs out
    async fn with_retry<Value, Call, CallFuture>(
        &self,
        method: &str,
        call: Call,
    ) -> Result<Value, RpcError>
    where
        Call: Fn(Arc<RpcClient>) -> CallFuture,
        CallFuture: Future<Output = Result<Value, RpcError>>,
    {
        self.with_retry_at(method, |index| call(self.clients[index].clone()))
            .await
    }

    /// Same failover, backoff and metrics, handing the callback the endpoint
    /// index so a caller can reach the url rather than the typed client.
    async fn with_retry_at<Value, Call, CallFuture>(
        &self,
        method: &str,
        call: Call,
    ) -> Result<Value, RpcError>
    where
        Call: Fn(usize) -> CallFuture,
        CallFuture: Future<Output = Result<Value, RpcError>>,
    {
        #[cfg(feature = "metrics")]
        let timer = tape_metrics::OperationTimer::new();

        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());
        let mut cursor = self.start_operation();

        loop {
            match tokio::time::timeout(self.config.timeout, call(cursor.index())).await {
                Ok(Ok(value)) => {
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = &self.metrics {
                        metrics.record_request(method, "success", timer.elapsed_secs());
                    }

                    return Ok(value);
                }
                Ok(Err(error)) => {
                    self.handle_error(method, error, &mut backoff, &mut cursor)
                        .await?;
                }
                Err(_elapsed) => {
                    self.handle_timeout(method, &mut backoff, &mut cursor)
                        .await?;
                }
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
            self.fail_over(cursor);
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
        self.fail_over(cursor);

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
            let display = tx_error.to_string();
            let message = if err_str.contains(&display) {
                err_str
            } else {
                format!("{display}; {err_str}")
            };
            return RpcError::Transaction {
                err: Some(tx_error),
                message,
                simulated: false,
            };
        }

        if err_str.contains("Transaction simulation failed") {
            return RpcError::Transaction {
                err: None,
                message: err_str,
                simulated: false,
            };
        }

        // Default to Request error with the error message
        RpcError::Request(err_str)
    }

    /// Convert a getTransaction ClientError, mapping lookup misses to
    /// TransactionNotFound.
    fn convert_transaction_error(err: ClientError, txid: Txid) -> RpcError {
        let err_str = err.to_string();
        if err_str.contains("not found") || err_str.contains("Transaction version not supported")
        {
            return RpcError::TransactionNotFound(txid);
        }

        Self::convert_error(err, None)
    }
}

/// The `getBlock` request body.
///
/// `json` encoding and full transaction details are what make the response
/// shape fixed enough to deserialise without the untagged enums. Rewards are
/// declined outright rather than dropped after parsing.
fn block_request(slot: u64, commitment: CommitmentLevel) -> Result<serde_json::Value, RpcError> {
    let config = RpcBlockConfig {
        commitment: Some(CommitmentConfig { commitment }),
        ..BLOCK_CONFIG
    };
    let config = serde_json::to_value(config)
        .map_err(|error| RpcError::Internal(format!("getBlock config: {error}")))?;

    Ok(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [slot, config],
    }))
}

/// Serialises to exactly the params `block_request` sends; commitment is
/// filled in per call from the configured level.
const BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    // Base64: the transaction arrives as bytes, not a tree of strings to walk.
    encoding: Some(UiTransactionEncoding::Base64),
    transaction_details: Some(TransactionDetails::Full),
    rewards: Some(false),
    commitment: None,
    max_supported_transaction_version: Some(0),
};

/// Fetch one block, deserialising the response body a single time.
///
/// The typed client walks the whole body into a `serde_json::Value` and back
/// out through `from_value`, so every field is built whether or not anything
/// reads it. Balances alone are 40% of a mainnet block. Here the body goes
/// straight into the fields the parser consumes.
async fn fetch_block(
    http: &reqwest::Client,
    endpoint: &str,
    slot: u64,
    commitment: CommitmentLevel,
) -> Result<Block, RpcError> {
    let response = http
        .post(endpoint)
        .json(&block_request(slot, commitment)?)
        .send()
        .await
        // reqwest renders transport failures as "error sending request ...",
        // which is already what the retriable and failover checks look for.
        .map_err(|error| RpcError::Request(error.to_string()))?;

    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| RpcError::Request(error.to_string()))?;

    if !status.is_success() {
        // Carries the status code, so 429 and 5xx stay retriable.
        return Err(RpcError::Request(format!(
            "getBlock http {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&body[..body.len().min(256)])
        )));
    }

    decode_block_response(&body)
}

/// Turn a `getBlock` response body into a block or the matching error.
fn decode_block_response(body: &[u8]) -> Result<Block, RpcError> {
    let envelope: BlockEnvelope = serde_json::from_slice(body)
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    if let Some(error) = envelope.error {
        // Skipped slots arrive here, and callers key on the message text, so it
        // is passed through rather than reworded.
        return Err(RpcError::Request(error.message));
    }

    // A null result is how a cluster reports a block it does not hold. The
    // typed path could only see this as a deserialisation failure.
    envelope.result.ok_or(RpcError::BlockNotAvailable)
}

#[derive(serde::Deserialize)]
struct BlockEnvelope {
    #[serde(default)]
    result: Option<Block>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

#[derive(serde::Deserialize)]
struct JsonRpcError {
    #[serde(default)]
    message: String,
}

/// Send a transaction and poll its signature status at a short fixed cadence.
/// The library confirm loop polls every 500ms, which adds up to half a slot of
/// pure sleep per transaction on top of confirmation itself.
/// Confirmation probe schedule: the ramp, then the steady cadence it settles to.
///
/// Overridable for cadence sweeps against a chain whose slot time has changed:
/// `TAPE_CONFIRM_RAMP_MS=5,15,50,130` and `TAPE_CONFIRM_POLL_MS=200` reproduce
/// the defaults. Parsed once; a malformed value falls back to the default
/// rather than failing the send.
fn confirm_cadence() -> (&'static [u64], u64) {
    static CADENCE: OnceLock<(Vec<u64>, u64)> = OnceLock::new();
    const DEFAULT_RAMP: [u64; 4] = [5, 15, 50, 130];
    const DEFAULT_POLL: u64 = 200;

    let (ramp, steady) = CADENCE.get_or_init(|| {
        let ramp = std::env::var("TAPE_CONFIRM_RAMP_MS")
            .ok()
            .and_then(|raw| {
                raw.split(',')
                    .map(|part| part.trim().parse::<u64>().ok())
                    .collect::<Option<Vec<_>>>()
            })
            .filter(|parsed: &Vec<u64>| !parsed.is_empty())
            .unwrap_or_else(|| DEFAULT_RAMP.to_vec());
        let steady = std::env::var("TAPE_CONFIRM_POLL_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(DEFAULT_POLL);
        (ramp, steady)
    });
    (ramp.as_slice(), *steady)
}

async fn send_and_poll(
    client: &RpcClient,
    transaction: &Transaction,
    commitment: CommitmentLevel,
    skip_preflight: bool,
) -> Result<Signature, ClientError> {
    // Probes before the steady cadence, aligned so the grid still lands on
    // 200 ms. A chain that confirms in single-digit milliseconds is caught by
    // the second probe instead of waiting out a full interval; a slot-paced
    // chain is caught at 200 and 400 exactly as before, so the slow case pays
    // four extra status calls and no extra latency.
    let (ramp, steady) = confirm_cadence();

    let commitment = CommitmentConfig { commitment };
    let signature = client
        .send_transaction_with_config(
            transaction,
            RpcSendTransactionConfig {
                // Hot write paths skip preflight (well-formed transactions,
                // failures read from signature status); other paths keep it.
                skip_preflight,
                ..RpcSendTransactionConfig::default()
            },
        )
        .await?;

    let mut probe = 0usize;
    loop {
        if let Some(status) = client
            .get_signature_status_with_commitment(&signature, commitment)
            .await?
        {
            status?;
            return Ok(signature);
        }

        let delay = ramp.get(probe).copied().unwrap_or(steady);
        probe += 1;
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
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
        self.with_retry("getSlot", |client| async move {
            client
                .get_slot()
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_finalized_slot(&self) -> Result<u64, RpcError> {
        let commitment = CommitmentConfig::finalized();

        self.with_retry("getSlot:finalized", move |client| async move {
            client
                .get_slot_with_commitment(commitment)
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_confirmed_slot(&self) -> Result<u64, RpcError> {
        let commitment = CommitmentConfig::confirmed();

        self.with_retry("getSlot:confirmed", move |client| async move {
            client
                .get_slot_with_commitment(commitment)
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_first_available_block(&self) -> Result<u64, RpcError> {
        self.with_retry("getFirstAvailableBlock", |client| async move {
            client
                .get_first_available_block()
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        self.with_retry("getLatestBlockhash", |client| async move {
            client
                .get_latest_blockhash()
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_genesis_hash(&self) -> Result<Hash, RpcError> {
        self.with_retry("getGenesisHash", |client| async move {
            client
                .get_genesis_hash()
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    async fn get_block(&self, slot: u64) -> Result<Block, RpcError> {
        self.with_retry_at("getBlock", |index| {
            fetch_block(
                &self.http,
                &self.config.endpoints[index],
                slot,
                self.config.commitment,
            )
        })
        .await
    }

    async fn get_transaction(
        &self,
        txid: &Txid,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, RpcError> {
        let txid = *txid;
        let signature: Signature = txid.into();
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig {
                commitment: self.config.commitment,
            }),
            max_supported_transaction_version: Some(0),
        };

        self.with_retry("getTransaction", move |client| async move {
            client
                .get_transaction_with_config(&signature, config)
                .await
                .map_err(|error| Self::convert_transaction_error(error, txid))
        })
        .await
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        self.with_retry("getBlockHeight", |client| async move {
            client
                .get_block_height()
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
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
        let pubkey = *pubkey;
        let solana_pubkey: Pubkey = pubkey.into();
        let commitment = CommitmentConfig { commitment };

        let response = self
            .with_retry("getAccount", move |client| async move {
                client
                    .get_account_with_commitment(&solana_pubkey, commitment)
                    .await
                    .map_err(|error| Self::convert_error(error, Some(pubkey)))
            })
            .await?;

        response.value.ok_or(RpcError::AccountNotFound(pubkey))
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
        let solana_pubkeys: Vec<Pubkey> = pubkeys.iter().copied().map(Into::into).collect();
        let commitment = CommitmentConfig { commitment };

        let response = self
            .with_retry("getMultipleAccounts", move |client| {
                let pubkeys = solana_pubkeys.clone();
                async move {
                    client
                        .get_multiple_accounts_with_commitment(&pubkeys, commitment)
                        .await
                        .map_err(|error| Self::convert_error(error, None))
                }
            })
            .await?;

        Ok(response.value)
    }

    async fn get_program_accounts(
        &self,
        program_id: &Address,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Address, Account)>, RpcError> {
        let solana_program_id: Pubkey = (*program_id).into();

        let accounts = self
            .with_retry("getProgramAccounts", move |client| {
                let config = config.clone();
                async move {
                    client
                        .get_program_ui_accounts_with_config(&solana_program_id, config)
                        .await
                        .map_err(|error| Self::convert_error(error, None))
                }
            })
            .await?;

        let mut out = Vec::with_capacity(accounts.len());
        for (address, account) in accounts {
            let account = account.to_account().ok_or_else(|| {
                RpcError::Deserialization("program account was not binary-decodable".to_string())
            })?;
            out.push((address.into(), account));
        }

        Ok(out)
    }

    async fn send_transaction(&self, transaction: &Transaction) -> Result<Txid, RpcError> {
        let transaction = Arc::new(transaction.clone());

        let signature = self
            .with_retry("sendTransaction", move |client| {
                let transaction = transaction.clone();
                async move {
                    client
                        .send_transaction(transaction.as_ref())
                        .await
                        .map_err(|error| Self::convert_error(error, None))
                }
            })
            .await?;

        Ok(signature.into())
    }

    async fn simulate_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<SimulationResult, RpcError> {
        let transaction = Arc::new(transaction.clone());
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig {
                commitment: self.config.commitment,
            }),
            ..RpcSimulateTransactionConfig::default()
        };

        self.with_retry("simulateTransaction", move |client| {
            let transaction = transaction.clone();
            let config = config.clone();
            async move {
                let response = client
                    .simulate_transaction_with_config(transaction.as_ref(), config)
                    .await
                    .map_err(|error| Self::convert_error(error, None))?;

                let value = response.value;
                Ok(SimulationResult {
                    err: value.err.map(Into::into),
                    units_consumed: value.units_consumed,
                })
            }
        })
        .await
    }

    async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
        commitment: CommitmentLevel,
        skip_preflight: bool,
    ) -> Result<Txid, RpcError> {
        let transaction = Arc::new(transaction.clone());

        let signature = self
            .with_retry("sendAndConfirmTransaction", move |client| {
                let transaction = transaction.clone();
                async move {
                    send_and_poll(&client, &transaction, commitment, skip_preflight)
                        .await
                        .map_err(|error| Self::convert_error(error, None))
                }
            })
            .await?;

        Ok(signature.into())
    }

    async fn get_signature_status(
        &self,
        txid: &Txid,
    ) -> Result<Option<Result<(), TransactionError>>, RpcError> {
        let signature: Signature = (*txid).into();

        self.with_retry("getSignatureStatus", move |client| async move {
            client
                .get_signature_status(&signature)
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }
}

fn flatten_error(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        let inner = inner.to_string();
        // The solana RPC error renders its full text at multiple levels of the
        // chain, so skip a source already contained in the message.
        if !out.contains(inner.as_str()) {
            out.push_str(": ");
            out.push_str(&inner);
        }
        source = source.and_then(|err| err.source());
    }
    redact_url_query(&drop_program_log_dump(&out))
}

/// Drop the solana program-execution log dump ("; N log messages: ...") the RPC
/// error appends. The classified instruction error is enough on its own; the
/// dump is duplicated across the error chain and only adds noise to node logs.
fn drop_program_log_dump(msg: &str) -> String {
    match msg.find("log messages:") {
        Some(pos) => msg[..pos]
            .trim_end()
            .trim_end_matches(|c: char| c.is_ascii_digit() || c == ';' || c == ' ')
            .to_string(),
        None => msg.to_string(),
    }
}

/// Strip query strings from URLs embedded in text; they carry credentials
/// (`?api=<key>`) and end up in logs.
pub fn redact_url_query(msg: &str) -> String {
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
    fn request_matches_the_config_the_typed_client_sent() {
        let request = block_request(77, CommitmentLevel::Confirmed).expect("config encodes");
        let params = &request["params"];

        assert_eq!(request["method"], "getBlock");
        assert_eq!(params[0], 77);
        assert_eq!(params[1]["encoding"], "base64");
        assert_eq!(params[1]["transactionDetails"], "full");
        assert_eq!(params[1]["rewards"], false);
        assert_eq!(params[1]["maxSupportedTransactionVersion"], 0);
        // Rendered by Display, so pin the wire spelling rather than trust it.
        assert_eq!(params[1]["commitment"], "confirmed");
    }

    #[test]
    fn null_result_reads_as_block_not_available() {
        let body = br#"{"jsonrpc":"2.0","result":null,"id":1}"#;

        assert!(matches!(
            decode_block_response(body),
            Err(RpcError::BlockNotAvailable)
        ));
    }

    #[test]
    fn skipped_slot_error_survives_as_a_skipped_slot() {
        let body = br#"{"jsonrpc":"2.0","error":{"code":-32007,
            "message":"Slot 12345 was skipped, or missing due to ledger jump to recent snapshot"},
            "id":1}"#;

        let error = decode_block_response(body).expect_err("skipped slot is an error");

        // The ingest loop skips the slot off this, so the message must survive.
        assert!(error.is_skipped_slot(), "got {error:?}");
    }

    #[test]
    fn rate_limit_error_stays_retriable() {
        let body = br#"{"jsonrpc":"2.0","error":{"code":429,"message":"Too Many Requests"},"id":1}"#;

        let error = decode_block_response(body).expect_err("rate limit is an error");

        assert!(error.is_retriable(), "got {error:?}");
    }

    #[test]
    fn result_decodes_into_the_parser_shape() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":{
            "blockhash":"abc","previousBlockhash":"def","parentSlot":41,"blockTime":1700,
            "rewards":[],
            "transactions":[{
              "transaction":["AQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUBAAECBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQEBAAMBAgM=","base64"],
              "meta":{"err":null,"preBalances":[1],"postBalances":[2],
                 "logMessages":["Program k1 invoke [1]"]}
            }]}}"#;

        let block = decode_block_response(body).expect("valid block");

        assert_eq!(block.blockhash, "abc");
        assert_eq!(block.parent_slot, 41);
        let txs = block.transactions.expect("transactions");
        assert!(!txs[0].is_failed());
        // Two keys: the fee payer, which is also the only account, and the
        // program the one instruction calls.
        assert_eq!(txs[0].transaction.message.account_keys.len(), 2);
        assert_eq!(txs[0].transaction.message.instructions.len(), 1);
    }

    #[test]
    fn garbage_body_is_a_deserialisation_error() {
        assert!(matches!(
            decode_block_response(b"not json"),
            Err(RpcError::Deserialization(_))
        ));
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
            RpcError::Transaction { err, message, .. } => {
                assert_eq!(
                    err,
                    Some(TransactionError::InstructionError(
                        1,
                        InstructionError::Custom(0x12),
                    ))
                );
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
