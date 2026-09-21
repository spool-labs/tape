//! Production Solana RPC client with retry and failover capabilities.

use crate::config::RpcConfig;
use crate::selector::{EndpointCursor, EndpointSelector};
use async_trait::async_trait;
use solana_account::Account;
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcTransactionConfig};
use solana_client::rpc_request::{RpcError as SolanaRpcError, RpcResponseErrorData};
use solana_client::rpc_response::RpcSimulateTransactionResult;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction::TransactionError;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionDetails,
    UiTransactionEncoding,
};
use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use tape_rpc::{BlockWindow, ChainKind, Rpc, RpcError, SimulationResult};
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
/// use tape_rpc_solana::{SolanaRpc, RpcConfig};
/// use tape_rpc::Rpc;
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
    /// Which chain the endpoints answer for, probed on first ask. A backend
    /// does not change kind under a client, so one probe settles it.
    chain_kind: tokio::sync::OnceCell<ChainKind>,
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
            chain_kind: tokio::sync::OnceCell::new(),
            #[cfg(feature = "metrics")]
            metrics,
        })
    }

    /// Which chain the endpoints behind this client answer for
    ///
    /// A tapesvm daemon serves `getMembership`; an agave validator answers it
    /// method not found. Anything that fails to answer at all is read as the
    /// open network, which costs nothing but the one round trip.
    async fn probe_chain_kind(&self) -> ChainKind {
        let own = self
            .with_retry_at("getMembership", |index| {
                probe_membership(&self.http, &self.config.endpoints[index])
            })
            .await
            .unwrap_or(false);

        match own {
            true => ChainKind::Own,
            false => ChainKind::Shared,
        }
    }

    /// Read the chain's own verdict for every entry it answered as replayed
    ///
    /// A retried post replays transactions the chain already ran, and the
    /// refusal says only that it ran them. The signature status says how each
    /// one ended, so a refused transaction whose reply was lost stays refused.
    /// An entry the chain holds no status for is left refused for the caller
    /// to send again.
    ///
    /// The status read stays on the endpoint the batch went to. Any other
    /// endpoint holds its own history, and an entry it has never seen would
    /// come back refused after it had already landed.
    async fn settle_replayed(
        &self,
        endpoint: usize,
        sent: &mut [Result<Txid, RpcError>],
        transactions: &[VersionedTransaction],
    ) {
        let mut replayed = Vec::new();
        for (index, verdict) in sent.iter().enumerate() {
            let Err(err) = verdict else { continue };
            if !is_already_processed(err) {
                continue;
            }
            let Some(signature) = transactions[index].signatures.first() else {
                continue;
            };
            replayed.push((index, *signature));
        }

        if replayed.is_empty() {
            return;
        }

        let signatures: Vec<Signature> = replayed.iter().map(|(_, sig)| *sig).collect();
        let Ok(statuses) = self.signature_statuses_at(endpoint, &signatures).await else {
            return;
        };

        for ((index, signature), status) in replayed.into_iter().zip(statuses) {
            // The chain has no status for it, so nothing here says it landed.
            let Some(status) = status else { continue };
            sent[index] = match status {
                Ok(()) => Ok(signature.into()),
                Err(error) => Err(RpcError::Transaction {
                    message: error.to_string(),
                    err: Some(error),
                    simulated: false,
                }),
            };
        }
    }

    /// The status one endpoint holds for each signature, in the order asked
    ///
    /// Retries stay on that endpoint, since a failover would answer from
    /// another node's history.
    async fn signature_statuses_at(
        &self,
        endpoint: usize,
        signatures: &[Signature],
    ) -> Result<Vec<Option<Result<(), TransactionError>>>, RpcError> {
        let client = self.clients[endpoint].clone();
        let mut backoff = tape_retry::Backoff::new(self.config.retry.to_retry_config());

        loop {
            let attempt = tokio::time::timeout(
                self.config.timeout,
                client.get_signature_statuses(signatures),
            )
            .await;

            let error = match attempt {
                Ok(Ok(response)) => {
                    return Ok(response
                        .value
                        .into_iter()
                        .map(|status| status.map(|status| status.status))
                        .collect())
                }
                Ok(Err(error)) => Self::convert_error(error, None),
                Err(_elapsed) => RpcError::Timeout(self.config.timeout),
            };

            if !error.is_retriable() {
                return Err(error);
            }

            match backoff.next_delay() {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(error),
            }
        }
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
        let (value, _endpoint) = self.with_retry_where(method, call).await?;
        Ok(value)
    }

    /// Same again, also reporting which endpoint answered, for a caller whose
    /// follow-up read only makes sense against that one.
    async fn with_retry_where<Value, Call, CallFuture>(
        &self,
        method: &str,
        call: Call,
    ) -> Result<(Value, usize), RpcError>
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

                    return Ok((value, cursor.index()));
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
    max_supported_transaction_version: Some(1),
};

/// Serialises to exactly the config `get_transaction` sends; commitment is
/// filled in per call from the configured level.
const TRANSACTION_CONFIG: RpcTransactionConfig = RpcTransactionConfig {
    // Base64: the event comes out of the log messages, and the tapesvm daemon refuses json.
    encoding: Some(UiTransactionEncoding::Base64),
    commitment: None,
    max_supported_transaction_version: Some(1),
};

/// Post one JSON-RPC body and hand the response body to `decode`.
///
/// The typed client walks the whole body into a `serde_json::Value` and back
/// out through `from_value`, so every field is built whether or not anything
/// reads it. Balances alone are 40% of a mainnet block. Here the body reaches
/// the decoder as bytes and goes straight into the fields it consumes.
async fn post_json<Decoded>(
    http: &reqwest::Client,
    endpoint: &str,
    method: &str,
    body: &serde_json::Value,
    decode: impl FnOnce(&[u8]) -> Result<Decoded, RpcError>,
) -> Result<Decoded, RpcError> {
    let response = http
        .post(endpoint)
        .json(body)
        .send()
        .await
        // reqwest renders transport failures as "error sending request ...",
        // which is already what the retriable and failover checks look for.
        .map_err(|error| RpcError::Request(error.to_string()))?;

    let status = response.status();
    let payload = response
        .bytes()
        .await
        .map_err(|error| RpcError::Request(error.to_string()))?;

    if !status.is_success() {
        // Holds the status code, so 429 and 5xx stay retriable.
        return Err(RpcError::Request(format!(
            "{method} http {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&payload[..payload.len().min(256)])
        )));
    }

    decode(&payload)
}

/// Fetch one block, deserialising the response body a single time.
async fn fetch_block(
    http: &reqwest::Client,
    endpoint: &str,
    slot: u64,
    commitment: CommitmentLevel,
) -> Result<Block, RpcError> {
    let request = block_request(slot, commitment)?;

    post_json(http, endpoint, "getBlock", &request, decode_block_response).await
}

/// Turn a `getBlock` response body into a block or the matching error.
fn decode_block_response(body: &[u8]) -> Result<Block, RpcError> {
    let envelope: BlockEnvelope = serde_json::from_slice(body)
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    if let Some(error) = envelope.error {
        return Err(match error.code {
            JSON_RPC_SERVER_ERROR_SLOT_SKIPPED => RpcError::SlotSkipped,
            // The chain has the block; this backend cannot serve it yet
            JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE => RpcError::BlockNotAvailable,
            // The rooted check declined to call it a skip, so this backend lost it
            JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED => RpcError::BlockPruned,
            _ => RpcError::Request(error.message),
        });
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
    code: i64,
    #[serde(default)]
    message: String,
    /// What agave puts the failed transaction's own error in
    #[serde(default)]
    data: Option<serde_json::Value>,
}

/// Ask one endpoint for its membership and read which chain answered.
///
/// Transport and HTTP failures come back as errors so the caller fails over;
/// a JSON-RPC refusal is an answer, and it says this is an agave validator.
async fn probe_membership(http: &reqwest::Client, endpoint: &str) -> Result<bool, RpcError> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": MEMBERSHIP_METHOD,
        "params": [],
    });

    post_json(http, endpoint, MEMBERSHIP_METHOD, &request, |body| {
        Ok(serves_membership(body))
    })
    .await
}

/// True when a getMembership reply came from a daemon that serves it
fn serves_membership(body: &[u8]) -> bool {
    let reply: MembershipReply = match serde_json::from_slice(body) {
        Ok(reply) => reply,
        Err(_) => return false,
    };

    reply.error.is_none() && reply.result.is_some()
}

/// What a getMembership post answers with, read only for which half is filled.
#[derive(serde::Deserialize)]
struct MembershipReply {
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

/// Build the JSON-RPC batch body for a run of sends, one call per
/// transaction with its index as the id.
fn send_batch_request(
    transactions: &[VersionedTransaction],
) -> Result<serde_json::Value, RpcError> {
    let mut calls = Vec::with_capacity(transactions.len());
    for (index, transaction) in transactions.iter().enumerate() {
        let encoded = wincode::serialize(transaction)
            .map_err(|error| RpcError::Internal(format!("encode transaction: {error}")))?;
        calls.push(serde_json::json!({
            "jsonrpc": "2.0",
            "id": index,
            "method": "sendTransaction",
            "params": [
                base64::encode(encoded),
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                },
            ],
        }));
    }

    Ok(serde_json::Value::Array(calls))
}

/// Post one batch of sends and read a verdict per transaction.
async fn send_batch(
    http: &reqwest::Client,
    endpoint: &str,
    body: &serde_json::Value,
    count: usize,
) -> Result<Vec<Result<Txid, RpcError>>, RpcError> {
    post_json(http, endpoint, "sendTransaction batch", body, |payload| {
        decode_send_batch_response(payload, count)
    })
    .await
}

/// Turn a batch reply into one verdict per call, placed by the id it answers.
///
/// JSON-RPC lets a server answer a batch in any order, so nothing is read off
/// an entry's position.
fn decode_send_batch_response(
    body: &[u8],
    count: usize,
) -> Result<Vec<Result<Txid, RpcError>>, RpcError> {
    let reply: SendBatchReply = serde_json::from_slice(body)
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    // A server that refuses the batch outright answers with one error object.
    let envelopes = match reply {
        SendBatchReply::Batch(envelopes) => envelopes,
        SendBatchReply::Refusal(failure) => {
            let message = match failure.error {
                Some(error) => error.message,
                None => "sendTransaction batch answered a single result".to_string(),
            };
            return Err(RpcError::Request(message));
        }
    };

    let mut verdicts: Vec<Option<Result<Txid, RpcError>>> = (0..count).map(|_| None).collect();
    for envelope in envelopes {
        let id = envelope.id.ok_or_else(|| {
            RpcError::Deserialization("sendTransaction batch answered without an id".into())
        })?;
        let slot = verdicts.get_mut(id).ok_or_else(|| {
            RpcError::Deserialization(format!(
                "sendTransaction batch answered id {id} of {count} calls"
            ))
        })?;
        if slot.is_some() {
            return Err(RpcError::Deserialization(format!(
                "sendTransaction batch answered id {id} twice"
            )));
        }
        *slot = Some(decode_send_entry(envelope.result, envelope.error));
    }

    let mut out = Vec::with_capacity(count);
    for (index, verdict) in verdicts.into_iter().enumerate() {
        out.push(verdict.ok_or_else(|| {
            RpcError::Deserialization(format!("sendTransaction batch left id {index} unanswered"))
        })?);
    }

    Ok(out)
}

/// One entry's verdict, classified the way a single send's rejection is.
fn decode_send_entry(
    result: Option<String>,
    error: Option<JsonRpcError>,
) -> Result<Txid, RpcError> {
    if let Some(error) = error {
        return Err(decode_entry_error(error));
    }

    let signature = result.ok_or_else(|| {
        RpcError::Deserialization("sendTransaction batch entry has neither result nor error".into())
    })?;
    let signature = Signature::from_str(&signature)
        .map_err(|error| RpcError::Deserialization(format!("send signature: {error}")))?;

    Ok(signature.into())
}

/// Read one entry's refusal as the program verdict it holds.
///
/// agave puts the failed transaction's own error under `data`, and a daemon
/// that executes the send outright puts it in the message. Either way the
/// entry comes back as a transaction error, so no caller reads the text.
fn decode_entry_error(error: JsonRpcError) -> RpcError {
    let data = error
        .data
        .and_then(|data| serde_json::from_value::<RpcSimulateTransactionResult>(data).ok())
        .map(RpcResponseErrorData::SendTransactionPreflightFailure)
        .unwrap_or(RpcResponseErrorData::Empty);

    let client_error = ClientError::from(ClientErrorKind::RpcError(
        SolanaRpcError::RpcResponseError {
            code: error.code,
            message: error.message,
            data,
        },
    ));

    let converted = SolanaRpc::convert_error(client_error, None);
    if let RpcError::Request(message) = &converted {
        if tape_rpc::looks_like_transaction_error(message) {
            return RpcError::Transaction {
                err: None,
                message: message.clone(),
                simulated: false,
            };
        }
    }

    converted
}

/// What a batch post answers with: one entry per call, or one refusal.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum SendBatchReply {
    Batch(Vec<SendEnvelope>),
    Refusal(SendEnvelope),
}

/// True when the chain is reporting a transaction it has already run.
///
/// A batch post that timed out or lost its answer is retried whole, so every
/// transaction of it that did run comes back this way. agave and the tapesvm
/// daemon word it differently, so both texts are matched.
fn is_already_processed(err: &RpcError) -> bool {
    let text = err.to_string();
    text.contains("already processed") || text.contains("already been processed")
}

#[derive(serde::Deserialize)]
struct SendEnvelope {
    #[serde(default)]
    id: Option<usize>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    error: Option<JsonRpcError>,
}

/// The method only a tapesvm daemon serves, which is what tells the two apart
const MEMBERSHIP_METHOD: &str = "getMembership";

/// agave's getBlock error codes
const JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE: i64 = -32004;
const JSON_RPC_SERVER_ERROR_SLOT_SKIPPED: i64 = -32007;
const JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;

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
    transaction: &VersionedTransaction,
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

    async fn chain_kind(&self) -> ChainKind {
        *self
            .chain_kind
            .get_or_init(|| self.probe_chain_kind())
            .await
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

    async fn get_blocks(&self, start: u64, end: u64) -> Result<Vec<u64>, RpcError> {
        let commitment = CommitmentConfig {
            commitment: self.config.commitment,
        };

        self.with_retry("getBlocks", move |client| async move {
            client
                .get_blocks_with_commitment(start, Some(end), commitment)
                .await
                .map_err(|error| Self::convert_error(error, None))
        })
        .await
    }

    /// Both calls run on one endpoint, so the tip that caps an empty window
    /// comes from the member that listed it.
    async fn get_block_window(&self, start: u64, end: u64) -> Result<BlockWindow, RpcError> {
        let commitment = CommitmentConfig {
            commitment: self.config.commitment,
        };

        self.with_retry_at("getBlocks", |index| {
            let client = self.clients[index].clone();
            async move {
                let produced = client
                    .get_blocks_with_commitment(start, Some(end), commitment)
                    .await
                    .map_err(|error| Self::convert_error(error, None))?;

                if !produced.is_empty() {
                    return Ok(BlockWindow { produced, tip: None });
                }

                let tip = client
                    .get_slot()
                    .await
                    .map_err(|error| Self::convert_error(error, None))?;

                Ok(BlockWindow {
                    produced,
                    tip: Some(tip),
                })
            }
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
            commitment: Some(CommitmentConfig {
                commitment: self.config.commitment,
            }),
            ..TRANSACTION_CONFIG
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

    async fn send_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<Txid, RpcError> {
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

    async fn send_transactions(
        &self,
        transactions: &[VersionedTransaction],
    ) -> Result<Vec<Result<Txid, RpcError>>, RpcError> {
        if transactions.is_empty() {
            return Ok(Vec::new());
        }

        let body = send_batch_request(transactions)?;
        let count = transactions.len();

        let (mut sent, endpoint) = self
            .with_retry_where("sendTransaction:batch", |index| {
                send_batch(
                    &self.http,
                    &self.config.endpoints[index],
                    &body,
                    count,
                )
            })
            .await?;

        self.settle_replayed(endpoint, &mut sent, transactions).await;

        Ok(sent)
    }

    async fn simulate_transaction(
        &self,
        transaction: &VersionedTransaction,
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
        transaction: &VersionedTransaction,
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
    use tape_rpc::looks_like_transaction_error;
    use solana_instruction_error::InstructionError;
    use solana_transaction::TransactionError;
    use crate::selector::EndpointStrategy;
    use std::time::Duration;

/// A stand-in JSON-RPC endpoint, answering one reply per method until dropped.
///
/// One request per connection, which is all reqwest needs to reach it.
struct MockEndpoint {
    url: String,
    served: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockEndpoint {
    async fn start<Answer>(answer: Answer) -> Self
    where
        Answer: Fn(&str) -> serde_json::Value + Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock endpoint binds");
        let url = format!("http://{}", listener.local_addr().expect("mock endpoint address"));
        let served = Arc::new(Mutex::new(Vec::new()));
        let answer = Arc::new(answer);

        let task = tokio::spawn({
            let served = served.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let answer = answer.clone();
                    let served = served.clone();
                    tokio::spawn(async move { serve_one(stream, answer, served).await });
                }
            }
        });

        Self { url, served, task }
    }

    /// Methods this endpoint answered, in the order they arrived
    fn served(&self) -> Vec<String> {
        self.served
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for MockEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Read one JSON-RPC post off the connection, answer it, and close.
async fn serve_one(
    mut stream: tokio::net::TcpStream,
    answer: Arc<dyn Fn(&str) -> serde_json::Value + Send + Sync>,
    served: Arc<Mutex<Vec<String>>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer = Vec::new();
    let mut scratch = [0u8; 4096];
    let body = loop {
        match stream.read(&mut scratch).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&scratch[..read]),
        }

        let Some(head_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buffer[..head_end]).to_lowercase();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let start = head_end + 4;
        if buffer.len() >= start + length {
            break buffer[start..start + length].to_vec();
        }
    };

    let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    // A batch post is an array of calls, and every call of one batch is the
    // same method here.
    let call = match request.as_array().and_then(|calls| calls.first()) {
        Some(first) => first,
        None => &request,
    };
    let method = call
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    served
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(method.clone());

    let payload = serde_json::to_vec(&answer(&method)).expect("mock reply encodes");
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.flush().await;
}

/// A client pointed at these mock endpoints, starting on the earliest one
fn client_for(endpoints: &[&MockEndpoint], strategy: EndpointStrategy) -> SolanaRpc {
    SolanaRpc::new(RpcConfig {
        endpoints: endpoints.iter().map(|mock| mock.url.clone()).collect(),
        strategy,
        timeout: Duration::from_secs(5),
        retry: crate::config::RpcRetryConfig {
            max_retries: 0,
            ..crate::config::RpcRetryConfig::default()
        },
        ..RpcConfig::default()
    })
    .expect("client builds")
}

/// What a tapesvm daemon answers a getMembership with
fn tapesvm_reply(method: &str) -> serde_json::Value {
    match method {
        "getMembership" => serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "version": 1, "members": [], "registered": [] },
        }),
        _ => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
    }
}

/// What an agave validator answers a method it does not have with
fn agave_reply(method: &str) -> serde_json::Value {
    match method {
        "getMembership" => serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": -32601, "message": "Method not found" },
        }),
        _ => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
    }
}

// a daemon that serves getMembership is a chain the operator runs
#[tokio::test]
async fn tapesvm_answer_takes_the_ordered_path() {
    let mock = MockEndpoint::start(tapesvm_reply).await;
    let client = client_for(&[&mock], EndpointStrategy::PreferPrimary);

    assert_eq!(client.chain_kind().await, ChainKind::Own);
    assert!(client.executes_batches_in_order().await);
    // The probe settles once, so a second ask costs no round trip.
    assert!(client.executes_batches_in_order().await);
    assert_eq!(mock.served(), vec!["getMembership".to_string()]);
}

// an agave validator answers a send before it executes, so its batch is unordered
#[tokio::test]
async fn agave_answer_stays_on_the_unordered_path() {
    let mock = MockEndpoint::start(agave_reply).await;
    let client = client_for(&[&mock], EndpointStrategy::PreferPrimary);

    assert_eq!(client.chain_kind().await, ChainKind::Shared);
    assert!(!client.executes_batches_in_order().await);
}

/// A stand-in endpoint that answers every post with one raw HTTP response
async fn serve_status(status: u16, body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock endpoint binds");
    let url = format!("http://{}", listener.local_addr().expect("mock endpoint address"));
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let head = format!(
                "HTTP/1.1 {status} Status\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
            let _ = stream.flush().await;
        }
    });

    (url, task)
}

// one post handler serves every raw call, and a refusal carries the method it refused
#[tokio::test]
async fn a_refused_post_holds_its_method_and_status() {
    let (url, task) = serve_status(429, "slow down").await;
    let http = reqwest::Client::new();

    let refused = post_json(&http, &url, "getBlock", &serde_json::json!({}), |_| {
        Ok::<(), RpcError>(())
    })
    .await
    .expect_err("a 429 is a refusal");

    assert_eq!(
        refused.to_string(),
        RpcError::Request("getBlock http 429: slow down".to_string()).to_string()
    );
    assert!(refused.is_retriable(), "a rate limit is a retry: {refused}");
    task.abort();
}

/// A member that holds no block in the window and reports this tip
fn empty_member(tip: u64) -> impl Fn(&str) -> serde_json::Value + Send + Sync + 'static {
    move |method: &str| match method {
        "getBlocks" => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": [] }),
        "getSlot" => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": tip }),
        _ => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
    }
}

// the tip capping an empty window comes from the member that listed it
#[tokio::test]
async fn an_empty_window_is_capped_by_the_member_that_listed_it() {
    let lagging = MockEndpoint::start(empty_member(1_000)).await;
    let current = MockEndpoint::start(empty_member(5_096)).await;
    let client = client_for(&[&lagging, &current], EndpointStrategy::RoundRobin);

    let first = client
        .get_block_window(1_000, 5_096)
        .await
        .expect("the window is listed");
    let second = client
        .get_block_window(1_000, 5_096)
        .await
        .expect("the window is listed");

    // Round robin starts the first pass on the second endpoint, so the two
    // passes read one member each.
    assert_eq!(first.tip, Some(5_096));
    assert_eq!(second.tip, Some(1_000));
    assert_eq!(lagging.served(), vec!["getBlocks".to_string(), "getSlot".to_string()]);
    assert_eq!(current.served(), vec!["getBlocks".to_string(), "getSlot".to_string()]);
}

/// One transaction with a signature the mock can answer for
fn signed_transaction(byte: u8) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![Signature::from([byte; 64])],
        ..VersionedTransaction::default()
    }
}

/// A chain that answers a replayed send, with this status for the signature
fn replaying_member(status: serde_json::Value) -> impl Fn(&str) -> serde_json::Value + Send + Sync + 'static {
    move |method: &str| match method {
        "getSignatureStatuses" => serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "context": { "slot": 100 }, "value": [status] },
        }),
        _ => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": null }),
    }
}

/// The batch reply agave sends for a transaction it has already run
fn already_processed_batch() -> serde_json::Value {
    serde_json::json!([{
        "jsonrpc": "2.0",
        "id": 0,
        "error": {
            "code": -32002,
            "message": "This transaction has already been processed",
        },
    }])
}

// a replayed send that ran clean the first time reads as landed
#[tokio::test]
async fn a_replayed_send_that_landed_reads_as_landed() {
    let landed = serde_json::json!({
        "slot": 100,
        "confirmations": serde_json::Value::Null,
        "err": serde_json::Value::Null,
        "status": { "Ok": serde_json::Value::Null },
        "confirmationStatus": "confirmed",
    });
    let batch = already_processed_batch();
    let member = MockEndpoint::start(move |method: &str| match method {
        "sendTransaction" => batch.clone(),
        other => replaying_member(landed.clone())(other),
    })
    .await;
    let client = client_for(&[&member], EndpointStrategy::PreferPrimary);

    let sent = client
        .send_transactions(&[signed_transaction(7)])
        .await
        .expect("the batch is answered");

    assert!(sent[0].is_ok(), "a clean replay is a landed entry: {sent:?}");
}

// a replayed send the chain refused the first time stays refused
#[tokio::test]
async fn a_replayed_send_that_failed_stays_refused() {
    let refused = serde_json::json!({
        "slot": 100,
        "confirmations": serde_json::Value::Null,
        "err": { "InstructionError": [0, { "Custom": 116 }] },
        "status": { "Err": { "InstructionError": [0, { "Custom": 116 }] } },
        "confirmationStatus": "confirmed",
    });
    let batch = already_processed_batch();
    let member = MockEndpoint::start(move |method: &str| match method {
        "sendTransaction" => batch.clone(),
        other => replaying_member(refused.clone())(other),
    })
    .await;
    let client = client_for(&[&member], EndpointStrategy::PreferPrimary);

    let sent = client
        .send_transactions(&[signed_transaction(9)])
        .await
        .expect("the batch is answered");

    let Err(error) = &sent[0] else {
        panic!("a refused first execution is not a landed entry: {sent:?}");
    };
    assert_eq!(error.custom_program_error(), Some(116));
}

// the endpoint that answered the batch also answers for its replayed entries
#[tokio::test]
async fn a_replayed_send_reads_its_own_endpoint() {
    let landed = serde_json::json!({
        "slot": 100,
        "confirmations": serde_json::Value::Null,
        "err": serde_json::Value::Null,
        "status": { "Ok": serde_json::Value::Null },
        "confirmationStatus": "confirmed",
    });
    let batch = already_processed_batch();
    // The ring's next endpoint ran none of these transactions, so it holds no
    // status for them.
    let other = MockEndpoint::start(replaying_member(serde_json::Value::Null)).await;
    let member = MockEndpoint::start(move |method: &str| match method {
        "sendTransaction" => batch.clone(),
        asked => replaying_member(landed.clone())(asked),
    })
    .await;
    let client = client_for(&[&other, &member], EndpointStrategy::RoundRobin);

    let sent = client
        .send_transactions(&[signed_transaction(11)])
        .await
        .expect("the batch is answered");

    assert!(sent[0].is_ok(), "a clean replay is a landed entry: {sent:?}");
    assert_eq!(
        member.served(),
        vec!["sendTransaction".to_string(), "getSignatureStatuses".to_string()]
    );
    assert!(other.served().is_empty(), "{:?}", other.served());
}

// a member holding the window keeps its tip to itself, which saves the round trip
#[tokio::test]
async fn a_listed_window_reads_no_tip() {
    let member = MockEndpoint::start(|method: &str| match method {
        "getBlocks" => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": [1_001, 1_002] }),
        _ => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": 5_096 }),
    })
    .await;
    let client = client_for(&[&member], EndpointStrategy::PreferPrimary);

    let window = client
        .get_block_window(1_000, 5_096)
        .await
        .expect("the window is listed");

    assert_eq!(window.produced, vec![1_001, 1_002]);
    assert_eq!(window.tip, None);
    assert_eq!(member.served(), vec!["getBlocks".to_string()]);
}

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
        assert_eq!(params[1]["maxSupportedTransactionVersion"], 1);
        // Rendered by Display, so pin the wire spelling rather than trust it.
        assert_eq!(params[1]["commitment"], "confirmed");
    }

    // json is refused by the tapesvm daemon, which leaves the register event unreadable
    #[test]
    fn transaction_request_asks_for_base64() {
        let config = RpcTransactionConfig {
            commitment: Some(CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            }),
            ..TRANSACTION_CONFIG
        };

        let params = serde_json::to_value(config).expect("config encodes");

        assert_eq!(params["encoding"], "base64");
        assert_eq!(params["maxSupportedTransactionVersion"], 1);
        assert_eq!(params["commitment"], "confirmed");
    }

    // a batch reply is placed by the id it answers, whatever order it arrives in
    #[test]
    fn batch_replies_land_by_id() {
        let first = Signature::from([1u8; 64]).to_string();
        let second = Signature::from([2u8; 64]).to_string();
        let third = Signature::from([3u8; 64]).to_string();
        let body = format!(
            r#"[{{"jsonrpc":"2.0","id":2,"result":"{third}"}},
                {{"jsonrpc":"2.0","id":0,"result":"{first}"}},
                {{"jsonrpc":"2.0","id":1,"result":"{second}"}}]"#
        );

        let verdicts = decode_send_batch_response(body.as_bytes(), 3).expect("batch decodes");

        let landed: Vec<String> = verdicts
            .into_iter()
            .map(|verdict| verdict.expect("entry sent").to_string())
            .collect();
        assert_eq!(landed, vec![first, second, third]);
    }

    // one rejected entry stays that entry's, and the sends around it still report their signatures
    #[test]
    fn batch_entry_error_stays_on_its_entry() {
        let first = Signature::from([1u8; 64]).to_string();
        let third = Signature::from([3u8; 64]).to_string();
        let body = format!(
            r#"[{{"jsonrpc":"2.0","id":0,"result":"{first}"}},
                {{"jsonrpc":"2.0","id":1,"error":{{"code":-32002,
                  "message":"Error processing Instruction 0: custom program error: 0x74"}}}},
                {{"jsonrpc":"2.0","id":2,"result":"{third}"}}]"#
        );

        let mut verdicts = decode_send_batch_response(body.as_bytes(), 3).expect("batch decodes");

        let rejected = verdicts.remove(1).expect_err("the middle entry was rejected");
        // A daemon that runs the send answers with the verdict in the message
        // and no data beside it, which is still a transaction error.
        assert!(
            matches!(rejected, RpcError::Transaction { err: None, .. }),
            "the program's rejection has to survive into the entry: {rejected}"
        );
        assert!(looks_like_transaction_error(&rejected.to_string()));
        assert!(verdicts[0].is_ok());
        assert!(verdicts[1].is_ok());
    }

    // agave puts the failed transaction's own error under the entry's data
    #[test]
    fn batch_entry_data_decodes_into_a_transaction_error() {
        let body = br#"[{"jsonrpc":"2.0","id":0,"error":{"code":-32002,
            "message":"Transaction simulation failed",
            "data":{"err":{"InstructionError":[0,{"Custom":116}]},
                    "logs":[],"accounts":null,"unitsConsumed":0}}}]"#;

        let mut verdicts = decode_send_batch_response(body, 1).expect("batch decodes");

        let rejected = verdicts.remove(0).expect_err("the entry was rejected");
        assert!(matches!(
            rejected,
            RpcError::Transaction { err: Some(_), .. }
        ));
        assert_eq!(rejected.custom_program_error(), Some(116));
    }

    // a replayed transaction the chain already ran reads as that transaction landing
    #[test]
    fn already_processed_reads_as_landed() {
        let refusal = RpcError::Request(
            "RPC response error -32002: transaction already processed; ".to_string(),
        );

        assert!(is_already_processed(&refusal));
        assert!(!is_already_processed(&RpcError::Request("boom".to_string())));
    }

    // agave words it its own way, and a retried batch has to read that too
    #[test]
    fn agave_already_processed_reads_as_replayed() {
        let refusal = RpcError::Request(
            "RPC response error -32002: This transaction has already been processed".to_string(),
        );

        assert!(is_already_processed(&refusal));
    }

    // a reply short of the calls it answers fails the whole batch
    #[test]
    fn batch_reply_must_answer_every_call() {
        let first = Signature::from([1u8; 64]).to_string();
        let body = format!(r#"[{{"jsonrpc":"2.0","id":0,"result":"{first}"}}]"#);

        assert!(decode_send_batch_response(body.as_bytes(), 2).is_err());
    }

    // every call of the batch asks for the same base64 send the single path does
    #[test]
    fn batch_request_shape() {
        let transaction = VersionedTransaction::default();

        let request = send_batch_request(&[transaction.clone(), transaction]).expect("encodes");

        assert_eq!(request[0]["id"], 0);
        assert_eq!(request[1]["id"], 1);
        assert_eq!(request[1]["method"], "sendTransaction");
        assert_eq!(request[1]["params"][1]["encoding"], "base64");
        assert_eq!(request[1]["params"][1]["skipPreflight"], true);
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

    // a slot the backend lost is retried in place
    #[test]
    fn long_term_storage_miss_retries_in_place() {
        let body = br#"{"jsonrpc":"2.0","error":{"code":-32009,
            "message":"Slot 12345 was skipped, or missing in long-term storage"},
            "id":1}"#;

        let error = decode_block_response(body).expect_err("a missing slot is an error");

        assert!(matches!(error, RpcError::BlockPruned), "got {error:?}");
        assert!(error.is_retriable(), "got {error:?}");
        assert!(!error.should_failover(), "got {error:?}");
        assert!(!error.is_skipped_slot(), "got {error:?}");
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
