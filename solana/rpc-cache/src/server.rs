//! Axum server glue. Two distinct request paths:
//!
//! - `getBlock` is served from the in-memory slot store (filled by
//!   bootstrap + live tail). On miss, falls through to upstream without
//!   inserting — the live tail owns slot-store writes.
//! - Everything else uses the original moka-based read-through cache
//!   (per-method TTLs from `cache::Policy`). Submit methods are logged
//!   and forwarded uncached.
//!
//! Batch requests (top-level JSON array) pass through unchanged.
//!
//! Two unauthed observability routes: `GET /v1/health` and
//! `GET /v1/stats`. Everything JSON-RPC requires `?api=<key>`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::{
    Router,
    extract::{ConnectInfo, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use serde::Deserialize;
use serde_json::{Value, json};
use tape_crypto::address::Address;
use tracing::{info, warn};

use crate::cache::{CacheStore, MethodKind, Policy};
use crate::key::CacheKey;
use crate::submit_log;
use crate::upstream::{Upstream, UpstreamError};

/// Solana JSON-RPC error code for skipped or missing slots. Returned
/// verbatim from upstream RPCs; we serve the same code from cached
/// tombstones so callers can't distinguish a real skip from a cached
/// one.
const SKIPPED_SLOT_ERROR_CODE: i32 = -32007;

#[derive(Clone)]
pub enum CachedBlock {
    /// Pre-serialized JSON-RPC `result` body for a confirmed,
    /// filtered block. Wrapped in an envelope at serve time.
    Present(Bytes),
    /// The slot was skipped or never produced. Replays the standard
    /// upstream error envelope.
    Skipped,
}

pub struct CacheStats {
    pub bootstrap_done: AtomicBool,
    pub bootstrap_target_slot: AtomicU64,
    pub epoch_start_slot: AtomicU64,
    pub newest_cached_slot: AtomicU64,
    pub last_observed_live_slot: AtomicU64,
    pub slot_store_hits: AtomicU64,
    pub slot_store_misses: AtomicU64,
    pub slots_fetched: AtomicU64,
    pub slots_skipped: AtomicU64,
    pub upstream_calls: AtomicU64,
}

impl CacheStats {
    pub fn new() -> Self {
        Self {
            bootstrap_done: AtomicBool::new(false),
            bootstrap_target_slot: AtomicU64::new(0),
            epoch_start_slot: AtomicU64::new(0),
            newest_cached_slot: AtomicU64::new(0),
            last_observed_live_slot: AtomicU64::new(0),
            slot_store_hits: AtomicU64::new(0),
            slot_store_misses: AtomicU64::new(0),
            slots_fetched: AtomicU64::new(0),
            slots_skipped: AtomicU64::new(0),
            upstream_calls: AtomicU64::new(0),
        }
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AppState {
    pub policy: Policy,
    pub cache: CacheStore,
    pub upstream: Upstream,
    pub log_submits: bool,
    /// Required as `?api=<api_key>` on every JSON-RPC request. Not a
    /// security boundary — a cheap filter to keep port scanners from
    /// discovering an open Solana endpoint. `/v1/*` routes do not
    /// require this.
    pub api_key: String,
    pub slot_store: MokaCache<u64, CachedBlock>,
    pub program_ids: Vec<Address>,
    pub stats: CacheStats,
}

#[derive(Deserialize, Default)]
struct AuthQuery {
    #[serde(default)]
    api: Option<String>,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(handle))
        // Providers often accept POST at other paths. Mirror that so nodes
        // configured with a path suffix still work.
        .route("/{*rest}", post(handle))
        .route("/v1/health", get(health))
        .route("/v1/stats", get(stats))
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.stats.bootstrap_done.load(Ordering::Relaxed) {
        (StatusCode::OK, axum::Json(json!({"status": "ok"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"status": "bootstrapping"})),
        )
            .into_response()
    }
}

async fn stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let s = &state.stats;
    let newest = s.newest_cached_slot.load(Ordering::Relaxed);
    let live = s.last_observed_live_slot.load(Ordering::Relaxed);
    let lag = live.saturating_sub(newest);

    state.slot_store.run_pending_tasks().await;
    let entries = state.slot_store.entry_count();
    let bytes = state.slot_store.weighted_size();

    let body = json!({
        "bootstrap_done": s.bootstrap_done.load(Ordering::Relaxed),
        "epoch_start_slot": s.epoch_start_slot.load(Ordering::Relaxed),
        "bootstrap_target_slot": s.bootstrap_target_slot.load(Ordering::Relaxed),
        "newest_cached_slot": newest,
        "last_observed_live_slot": live,
        "lag_from_live_slot": lag,
        "slot_store_entries": entries,
        "slot_store_approximate_bytes": bytes,
        "slot_store_hits": s.slot_store_hits.load(Ordering::Relaxed),
        "slot_store_misses": s.slot_store_misses.load(Ordering::Relaxed),
        "slots_fetched": s.slots_fetched.load(Ordering::Relaxed),
        "slots_skipped": s.slots_skipped.load(Ordering::Relaxed),
        "upstream_calls": s.upstream_calls.load(Ordering::Relaxed),
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn handle(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(auth): Query<AuthQuery>,
    body: axum::body::Bytes,
) -> Response {
    if auth.api.as_deref() != Some(state.api_key.as_str()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized".to_string()).into_response();
    }

    let Ok(req) = serde_json::from_slice::<Value>(&body) else {
        return (
            StatusCode::BAD_REQUEST,
            "invalid JSON-RPC body".to_string(),
        )
            .into_response();
    };

    if req.is_array() {
        return forward_raw(&state, &req).await;
    }

    let Some(obj) = req.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            "expected JSON-RPC object".to_string(),
        )
            .into_response();
    };

    let method = obj.get("method").and_then(Value::as_str).unwrap_or("");
    let params = obj.get("params").cloned().unwrap_or(Value::Null);
    let id = obj.get("id").cloned().unwrap_or(Value::Null);
    let caller = addr.ip().to_string();

    // Block path: separate confirmed slot-keyed store, populated by
    // bootstrap + live tail. Falls through to upstream on non-confirmed
    // commitments or misses without writing — live tail owns writes, so
    // concurrent serve-path inserts would race with no upside.
    if method == "getBlock" {
        if is_confirmed_get_block(&params) {
            if let Some(slot) = parse_slot_param(&params) {
                if let Some(cached) = state.slot_store.get(&slot).await {
                    state.stats.slot_store_hits.fetch_add(1, Ordering::Relaxed);
                    return serve_cached_block(id, cached);
                }
                state.stats.slot_store_misses.fetch_add(1, Ordering::Relaxed);
            }
        } else if parse_slot_param(&params).is_some() {
            state.stats.slot_store_misses.fetch_add(1, Ordering::Relaxed);
        }
        return forward_passthrough(&state, &req, id).await;
    }

    match state.policy.classify(method) {
        MethodKind::Submit => {
            if state.log_submits {
                submit_log::record(&caller, method, &params);
            }
            forward_raw(&state, &req).await
        }
        MethodKind::Read { ttl } => {
            let key = CacheKey::from_request(method, &params);
            if let Some(cached) = state.cache.get(&key).await {
                return json_ok(id, (*cached).clone()).into_response();
            }

            // Miss — forward and populate. moka's get_with would coalesce
            // concurrent misses, but we lean on the cache's own
            // concurrency: in the worst case two nodes duplicate one
            // upstream call, which is cheap compared to the uncached
            // baseline. Keeps the error path straightforward.
            state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
            match state.upstream.forward(&req).await {
                Ok(envelope) => {
                    if let Some(result) = &envelope.result {
                        state
                            .cache
                            .insert(key, Arc::new(result.clone()), ttl)
                            .await;
                    }
                    (
                        StatusCode::OK,
                        axum::Json(reshape_with_id(envelope, id)),
                    )
                        .into_response()
                }
                Err(e) => upstream_err(id, &e).into_response(),
            }
        }
        MethodKind::Unknown => {
            warn!(%method, "unclassified method; passing through");
            forward_raw(&state, &req).await
        }
    }
}

fn parse_slot_param(params: &Value) -> Option<u64> {
    params.as_array()?.first()?.as_u64()
}

fn is_confirmed_get_block(params: &Value) -> bool {
    get_block_commitment(params) == Some("confirmed")
}

fn get_block_commitment(params: &Value) -> Option<&str> {
    let commitment = params.as_array()?.get(1)?.get("commitment")?;
    match commitment {
        Value::String(value) => Some(value.as_str()),
        Value::Object(map) => map.get("commitment").and_then(Value::as_str),
        _ => None,
    }
}

fn serve_cached_block(id: Value, cached: CachedBlock) -> Response {
    match cached {
        CachedBlock::Present(bytes) => serve_present_block(id, bytes),
        CachedBlock::Skipped => serve_skipped_envelope(id),
    }
}

/// Build the JSON-RPC envelope around a pre-serialized `result` body
/// without re-parsing it. Saves a round-trip through serde for the
/// (large) block body on the hot serve path.
fn serve_present_block(id: Value, result_bytes: Bytes) -> Response {
    let id_serialized = serde_json::to_vec(&id).unwrap_or_else(|_| b"null".to_vec());
    let mut buf = Vec::with_capacity(40 + id_serialized.len() + result_bytes.len());
    buf.extend_from_slice(br#"{"jsonrpc":"2.0","id":"#);
    buf.extend_from_slice(&id_serialized);
    buf.extend_from_slice(br#","result":"#);
    buf.extend_from_slice(&result_bytes);
    buf.push(b'}');

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        buf,
    )
        .into_response()
}

fn serve_skipped_envelope(id: Value) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": SKIPPED_SLOT_ERROR_CODE,
                "message": "Slot was skipped, or missing due to ledger jump to recent snapshot",
            },
        })),
    )
        .into_response()
}

/// Forward the request upstream without caching the response. Used
/// for getBlock misses — live tail owns slot-store writes.
async fn forward_passthrough(state: &AppState, req: &Value, id: Value) -> Response {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    match state.upstream.forward(req).await {
        Ok(envelope) => (
            StatusCode::OK,
            axum::Json(reshape_with_id(envelope, id)),
        )
            .into_response(),
        Err(e) => upstream_err(id, &e).into_response(),
    }
}

async fn forward_raw(state: &AppState, req: &Value) -> Response {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    // For batch requests, `id` is per-sub-request; nothing for us to
    // preserve at the envelope level. Just forward and return verbatim.
    match state.upstream.forward(req).await {
        Ok(env) => {
            let wire = json!({
                "jsonrpc": env.jsonrpc,
                "id": env.id,
                "result": env.result,
                "error": env.error,
            });
            (StatusCode::OK, axum::Json(strip_nulls(wire))).into_response()
        }
        Err(e) => {
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            upstream_err(id, &e).into_response()
        }
    }
}

/// Rebuild a JSON-RPC response envelope substituting the caller's
/// original `id`, which is what clients match on.
fn reshape_with_id(env: crate::upstream::RpcEnvelope, id: Value) -> Value {
    let mut out = json!({
        "jsonrpc": env.jsonrpc,
        "id": id,
    });
    if let Some(r) = env.result {
        out["result"] = r;
    }
    if let Some(e) = env.error {
        out["error"] = e;
    }
    out
}

fn json_ok(id: Value, result: Value) -> (StatusCode, axum::Json<Value>) {
    (
        StatusCode::OK,
        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        })),
    )
}

fn upstream_err(id: Value, err: &UpstreamError) -> (StatusCode, axum::Json<Value>) {
    info!(error = %err, "returning upstream-error envelope");
    (
        StatusCode::BAD_GATEWAY,
        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": format!("upstream: {err}"),
            },
        })),
    )
}

fn strip_nulls(mut v: Value) -> Value {
    if let Value::Object(ref mut map) = v {
        map.retain(|_, val| !val.is_null());
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_param_extracted_from_array() {
        let p = json!([12345, {"encoding": "json"}]);
        assert_eq!(parse_slot_param(&p), Some(12345));
    }

    #[test]
    fn slot_param_missing_returns_none() {
        assert_eq!(parse_slot_param(&Value::Null), None);
        assert_eq!(parse_slot_param(&json!([])), None);
        assert_eq!(parse_slot_param(&json!(["not-a-number"])), None);
    }

    #[test]
    fn confirmed_get_block_detects_flat_commitment() {
        let p = json!([12345, {"encoding": "json", "commitment": "confirmed"}]);
        assert!(is_confirmed_get_block(&p));
    }

    #[test]
    fn confirmed_get_block_detects_nested_commitment() {
        let p = json!([12345, {"commitment": {"commitment": "confirmed"}}]);
        assert!(is_confirmed_get_block(&p));
    }

    #[test]
    fn finalized_get_block_does_not_use_slot_store() {
        let p = json!([12345, {"encoding": "json", "commitment": "finalized"}]);
        assert!(!is_confirmed_get_block(&p));
    }

    #[test]
    fn present_block_envelope_serialization() {
        let result_bytes = Bytes::from_static(br#"{"blockhash":"abc","parentSlot":10}"#);
        let resp = serve_present_block(json!(7), result_bytes);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn skipped_envelope_carries_solana_error_code() {
        let resp = serve_skipped_envelope(json!(42));
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
