//! Axum server glue. Three distinct request paths:
//!
//! - `getBlock` is served from the in-memory slot store (filled by
//!   bootstrap + live tail), in the encoding the request asks for: json,
//!   the default, or base64. A confirmed miss fills the store from
//!   upstream. Other encodings and commitments pass through to upstream.
//! - A confirmed `getSlot` is answered with the slot the live tail holds
//!   every block up to, while that reading is recent.
//! - Everything else uses the original moka-based read-through cache
//!   (per-method TTLs from `cache::Policy`). Submit methods are logged
//!   and forwarded uncached.
//!
//! Batch requests (top-level JSON array) pass through unchanged.
//!
//! A public instance sends every JSON-RPC request down a third path,
//! `public::serve_public`, which meters the caller and gates the method.
//!
//! Two unauthed observability routes: `GET /v1/health` and
//! `GET /v1/stats`. Every fleet JSON-RPC request requires `?api=<key>`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::{
    Router,
    extract::{ConnectInfo, DefaultBodyLimit, Query, State},
    http::{HeaderMap, StatusCode, header},
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
use crate::public::{PUBLIC_BODY_LIMIT, Public, serve_public};
use crate::runtime::fetch_slot_block;
use crate::submit_log;
use crate::upstream::{CALLER_RATE_LIMIT_WAIT, RpcEnvelope, Upstream, UpstreamError};

/// Solana JSON-RPC error code for skipped or missing slots. Returned
/// verbatim from upstream RPCs; we serve the same code from cached
/// tombstones so callers can't distinguish a real skip from a cached
/// one.
const SKIPPED_SLOT_ERROR_CODE: i32 = -32007;

/// A confirmed `getSlot` is answered from the live tail while its reading is
/// this recent, which is as long as a cached answer lives. Past it the request
/// goes upstream, so a tail that has stopped shows as an upstream fault.
const TAIL_FRESH_FOR: Duration = Duration::from_secs(2);

/// Held in `tail_read_ms` until the live tail records its first reading
const NO_TAIL_READING: u64 = u64::MAX;

/// The transaction encodings the slot store can answer
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockEncoding {
    Json,
    Base64,
}

#[derive(Clone)]
pub enum CachedBlock {
    /// Pre-serialized JSON-RPC `result` bodies for a confirmed, filtered
    /// block, one per encoding. Wrapped in an envelope at serve time.
    Present { base64: Bytes, json: Bytes },
    /// The slot was skipped or never produced. Replays the standard
    /// upstream error envelope.
    Skipped,
}

pub struct CacheStats {
    pub bootstrap_done: AtomicBool,
    pub bootstrap_target_slot: AtomicU64,
    pub bootstrap_fill_start_slot: AtomicU64,
    pub epoch_start_slot: AtomicU64,
    pub newest_cached_slot: AtomicU64,
    pub last_observed_live_slot: AtomicU64,
    pub tail_slot: AtomicU64,
    tail_read_ms: AtomicU64,
    pub slot_store_hits: AtomicU64,
    pub slot_store_misses: AtomicU64,
    pub slots_fetched: AtomicU64,
    pub slots_skipped: AtomicU64,
    pub upstream_calls: AtomicU64,
    pub public_refused: AtomicU64,
    pub public_callers_limited: AtomicU64,
    pub public_upstream_limited: AtomicU64,
    started: Instant,
}

impl CacheStats {
    pub fn new() -> Self {
        Self::started_at(Instant::now())
    }

    /// Stats whose clock began at `started`, so tests can age a tail reading
    pub fn started_at(started: Instant) -> Self {
        Self {
            bootstrap_done: AtomicBool::new(false),
            bootstrap_target_slot: AtomicU64::new(0),
            bootstrap_fill_start_slot: AtomicU64::new(0),
            epoch_start_slot: AtomicU64::new(0),
            newest_cached_slot: AtomicU64::new(0),
            last_observed_live_slot: AtomicU64::new(0),
            tail_slot: AtomicU64::new(0),
            tail_read_ms: AtomicU64::new(NO_TAIL_READING),
            slot_store_hits: AtomicU64::new(0),
            slot_store_misses: AtomicU64::new(0),
            slots_fetched: AtomicU64::new(0),
            slots_skipped: AtomicU64::new(0),
            upstream_calls: AtomicU64::new(0),
            public_refused: AtomicU64::new(0),
            public_callers_limited: AtomicU64::new(0),
            public_upstream_limited: AtomicU64::new(0),
            started,
        }
    }

    /// Record that the live tail holds every block up to `slot`
    pub fn record_tail(&self, slot: u64) {
        self.tail_slot.store(slot, Ordering::Relaxed);
        self.tail_read_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// The live tail's slot while its reading is recent
    pub fn fresh_tail_slot(&self) -> Option<u64> {
        let read_ms = self.tail_read_ms.load(Ordering::Relaxed);
        if read_ms == NO_TAIL_READING {
            return None;
        }

        let now_ms = self.started.elapsed().as_millis() as u64;
        let is_fresh = now_ms.saturating_sub(read_ms) <= TAIL_FRESH_FOR.as_millis() as u64;
        is_fresh.then(|| self.tail_slot.load(Ordering::Relaxed))
    }

    /// Backfill rate/ETA derived from progress since process start. Only
    /// meaningful while bootstrapping; `None` once done.
    pub fn bootstrap_rate(&self) -> Option<(f64, u64)> {
        if self.bootstrap_done.load(Ordering::Relaxed) {
            return None;
        }

        let fill_start = self.bootstrap_fill_start_slot.load(Ordering::Relaxed);
        let newest = self.newest_cached_slot.load(Ordering::Relaxed);
        let target = self.bootstrap_target_slot.load(Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_secs_f64();
        if newest <= fill_start || elapsed <= 0.0 {
            return None;
        }

        let slots_per_sec = (newest - fill_start) as f64 / elapsed;
        let eta_secs = (target.saturating_sub(newest) as f64 / slots_per_sec) as u64;
        Some((slots_per_sec, eta_secs))
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
    /// Required as `?api=<api_key>` on every fleet JSON-RPC request. Not a
    /// security boundary — a cheap filter to keep port scanners from
    /// discovering an open Solana endpoint. `/v1/*` routes do not
    /// require this, and a public instance has none.
    pub api_key: Option<String>,
    pub public: Option<Public>,
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
    let is_public = state.public.is_some();
    let router = Router::new()
        .route("/", post(handle))
        // Providers often accept POST at other paths. Mirror that so nodes
        // configured with a path suffix still work.
        .route("/{*rest}", post(handle))
        .route("/v1/health", get(health))
        .route("/v1/stats", get(stats))
        .with_state(state);
    if is_public {
        router.layer(DefaultBodyLimit::max(PUBLIC_BODY_LIMIT))
    } else {
        router
    }
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

    let (slots_per_sec, eta_secs) = match s.bootstrap_rate() {
        Some((rate, eta)) => ((rate * 10.0).round() / 10.0, Some(eta)),
        None => (0.0, None),
    };

    let body = json!({
        "bootstrap_done": s.bootstrap_done.load(Ordering::Relaxed),
        "epoch_start_slot": s.epoch_start_slot.load(Ordering::Relaxed),
        "bootstrap_target_slot": s.bootstrap_target_slot.load(Ordering::Relaxed),
        "bootstrap_fill_start_slot": s.bootstrap_fill_start_slot.load(Ordering::Relaxed),
        "bootstrap_slots_per_sec": slots_per_sec,
        "bootstrap_eta_secs": eta_secs,
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
        "public_refused": s.public_refused.load(Ordering::Relaxed),
        "public_callers_limited": s.public_callers_limited.load(Ordering::Relaxed),
        "public_upstream_limited": s.public_upstream_limited.load(Ordering::Relaxed),
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn handle(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(auth): Query<AuthQuery>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(public) = &state.public {
        return serve_public(&state, public, addr.ip(), &headers, &body).await;
    }
    if auth.api.is_none() || auth.api != state.api_key {
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
    // bootstrap + live tail. Confirmed misses fill the same store, so
    // replay ranges outside the boot window are still shared fleet-wide.
    if method == "getBlock" {
        if is_confirmed_get_block(&params) {
            if let (Some(slot), Some(encoding)) = (parse_slot_param(&params), get_block_encoding(&params)) {
                if let Some(cached) = state.slot_store.get(&slot).await {
                    state.stats.slot_store_hits.fetch_add(1, Ordering::Relaxed);
                    return serve_cached_block(id, cached, encoding);
                }
                state.stats.slot_store_misses.fetch_add(1, Ordering::Relaxed);
                return fill_and_serve_cached_block(&state, id, slot, encoding).await;
            }
        } else if parse_slot_param(&params).is_some() {
            state.stats.slot_store_misses.fetch_add(1, Ordering::Relaxed);
        }
        return forward_passthrough(&state, &req, id).await;
    }

    // The live tail holds every block up to its slot, so this tip moves a
    // slot at a time and never runs ahead of the store.
    if method == "getSlot" && commitment_at(&params, 0) == Some("confirmed") {
        if let Some(slot) = state.stats.fresh_tail_slot() {
            return (
                StatusCode::OK,
                axum::Json(json!({"jsonrpc": "2.0", "id": id, "result": slot})),
            )
                .into_response();
        }
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
                return serve_cached_read(id, &cached);
            }

            // Miss — forward and populate. moka's get_with would coalesce
            // concurrent misses, but we lean on the cache's own
            // concurrency: in the worst case two nodes duplicate one
            // upstream call, which is cheap compared to the uncached
            // baseline. Keeps the error path straightforward.
            state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
            match state.upstream.forward(&req, CALLER_RATE_LIMIT_WAIT).await {
                Ok(envelope) => {
                    if let Some(result) = &envelope.result {
                        if let Ok(serialized) = serde_json::to_vec(result) {
                            state.cache.insert(key, Bytes::from(serialized), ttl).await;
                        }
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
    commitment_at(params, 1) == Some("confirmed")
}

/// The commitment in the config object at `position` of a request's params
fn commitment_at(params: &Value, position: usize) -> Option<&str> {
    let commitment = params.as_array()?.get(position)?.get("commitment")?;
    match commitment {
        Value::String(value) => Some(value.as_str()),
        Value::Object(map) => map.get("commitment").and_then(Value::as_str),
        _ => None,
    }
}

/// The transaction encoding a `getBlock` asks for: json when absent, as
/// the rpc defaults it; nothing for an encoding the store does not hold
fn get_block_encoding(params: &Value) -> Option<BlockEncoding> {
    let config = params.as_array()?.get(1);
    let encoding = config.and_then(|config| config.get("encoding"));
    match encoding.and_then(Value::as_str) {
        None => Some(BlockEncoding::Json),
        Some("json") => Some(BlockEncoding::Json),
        Some("base64") => Some(BlockEncoding::Base64),
        Some(_) => None,
    }
}

fn serve_cached_block(id: Value, cached: CachedBlock, encoding: BlockEncoding) -> Response {
    match cached {
        CachedBlock::Present { base64, json } => match encoding {
            BlockEncoding::Json => serve_present_block(id, json),
            BlockEncoding::Base64 => serve_present_block(id, base64),
        },
        CachedBlock::Skipped => serve_skipped_envelope(id),
    }
}

async fn fill_and_serve_cached_block(
    state: &Arc<AppState>,
    id: Value,
    slot: u64,
    encoding: BlockEncoding,
) -> Response {
    let fetch_state = Arc::clone(state);
    match state
        .slot_store
        .try_get_with(slot, async move {
            fetch_slot_block(fetch_state.as_ref(), slot, CALLER_RATE_LIMIT_WAIT).await
        })
        .await
    {
        Ok(cached) => serve_cached_block(id, cached, encoding),
        Err(error) => slot_fill_err(id, error.as_str()).into_response(),
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

/// Forward the request upstream without caching the response. Used for
/// non-confirmed `getBlock` requests and general passthrough paths.
async fn forward_passthrough(state: &AppState, req: &Value, id: Value) -> Response {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    match state.upstream.forward(req, CALLER_RATE_LIMIT_WAIT).await {
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
    match state.upstream.forward(req, CALLER_RATE_LIMIT_WAIT).await {
        Ok(env) => raw_reply(env),
        Err(e) => {
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            upstream_err(id, &e).into_response()
        }
    }
}

/// Return the envelope as upstream sent it, id included.
pub fn raw_reply(env: RpcEnvelope) -> Response {
    let wire = json!({
        "jsonrpc": env.jsonrpc,
        "id": env.id,
        "result": env.result,
        "error": env.error,
    });
    (StatusCode::OK, axum::Json(strip_nulls(wire))).into_response()
}

/// Rebuild a JSON-RPC response envelope substituting the caller's
/// original `id`, which is what clients match on.
pub fn reshape_with_id(env: RpcEnvelope, id: Value) -> Value {
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

/// Splice a cached `result` body into the envelope, keys in serde_json's sorted order
pub fn serve_cached_read(id: Value, result: &[u8]) -> Response {
    let id_serialized = serde_json::to_vec(&id).unwrap_or_else(|_| b"null".to_vec());
    let mut buf = Vec::with_capacity(40 + id_serialized.len() + result.len());
    buf.extend_from_slice(br#"{"id":"#);
    buf.extend_from_slice(&id_serialized);
    buf.extend_from_slice(br#","jsonrpc":"2.0","result":"#);
    buf.extend_from_slice(result);
    buf.push(b'}');

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        buf,
    )
        .into_response()
}

pub fn upstream_err(id: Value, err: &UpstreamError) -> (StatusCode, axum::Json<Value>) {
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

fn slot_fill_err(id: Value, err: &str) -> (StatusCode, axum::Json<Value>) {
    info!(error = %err, "returning slot-fill-error envelope");
    (
        StatusCode::BAD_GATEWAY,
        axum::Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": format!("upstream getBlock: {err}"),
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

    // the encoding defaults to json, base64 is served, the rest pass through
    #[test]
    fn encoding_from_params() {
        assert_eq!(get_block_encoding(&json!([1, {"commitment": "confirmed"}])), Some(BlockEncoding::Json));
        assert_eq!(get_block_encoding(&json!([1, {"encoding": "json"}])), Some(BlockEncoding::Json));
        assert_eq!(get_block_encoding(&json!([1, {"encoding": "base64"}])), Some(BlockEncoding::Base64));
        assert_eq!(get_block_encoding(&json!([1, {"encoding": "jsonParsed"}])), None);
        assert_eq!(get_block_encoding(&json!([1, {"encoding": "base58"}])), None);
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

    // a getSlot carries its commitment in the first param, flat or nested
    #[test]
    fn slot_commitment() {
        assert_eq!(commitment_at(&json!([{"commitment": "confirmed"}]), 0), Some("confirmed"));
        assert_eq!(commitment_at(&json!([{"commitment": {"commitment": "finalized"}}]), 0), Some("finalized"));
        assert_eq!(commitment_at(&json!([]), 0), None);
        assert_eq!(commitment_at(&Value::Null, 0), None);
    }

    // the tail's slot is served only while its reading is recent
    #[test]
    fn stale_tail() {
        let stats = CacheStats::started_at(Instant::now() - TAIL_FRESH_FOR * 3);
        assert_eq!(stats.fresh_tail_slot(), None);

        stats.record_tail(100);
        assert_eq!(stats.fresh_tail_slot(), Some(100));

        stats.tail_read_ms.store(0, Ordering::Relaxed);
        assert_eq!(stats.fresh_tail_slot(), None);
    }

    #[test]
    fn present_block_envelope_serialization() {
        let result_bytes = Bytes::from_static(br#"{"blockhash":"abc","parentSlot":10}"#);
        let resp = serve_present_block(json!(7), result_bytes);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // the old hit path, a cloned tree serialized behind the envelope
    fn old_hit(id: Value, result: &Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"jsonrpc": "2.0", "id": id, "result": result.clone()}))
            .expect("serialize envelope")
    }

    fn body(response: Response) -> Bytes {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .expect("body")
    }

    // a spliced hit sends the bytes the cloned tree did
    #[test]
    fn cached_read_bytes() {
        let results = [
            json!({"context": {"slot": 9}, "value": [{"data": ["AAEC", "base64"], "lamports": 5}, null]}),
            json!(123_456_789u64),
            json!("4uQeVj5tqViQh7yWWGStvkEG1Zmhx6uasJtWCJziofM"),
            Value::Null,
        ];
        let ids = [json!(1), json!("abc"), Value::Null];

        for result in &results {
            for id in &ids {
                let cached = serde_json::to_vec(result).expect("serialize result");
                let spliced = body(serve_cached_read(id.clone(), &cached));
                assert_eq!(spliced.as_ref(), old_hit(id.clone(), result).as_slice());
            }
        }
    }

    // a run of read-cache hits on an account batch
    // cargo test -p rpc-cache --release --lib hit_loop -- --ignored --nocapture
    #[test]
    #[ignore]
    fn hit_loop() {
        let accounts: Vec<Value> = (0..100)
            .map(|index| {
                json!({
                    "data": [format!("{index:0>270}"), "base64"],
                    "executable": false,
                    "lamports": 1_000_000 + index,
                    "owner": "tape9hFAZTFUWyqLb7CpkbXkPbAr1ubwATCVeQr8cW7",
                    "rentEpoch": 18_446_744_073_709_551_615u64,
                    "space": 200,
                })
            })
            .collect();
        let result = json!({"context": {"apiVersion": "2.3.0", "slot": 400_000_000}, "value": accounts});
        let cached = Bytes::from(serde_json::to_vec(&result).expect("serialize"));
        let hits = 20_000;

        let started = Instant::now();
        let mut sent = 0usize;
        for hit in 0..hits {
            let response = serve_cached_read(json!(hit), &cached);
            sent += usize::from(response.status() == StatusCode::OK);
        }
        let elapsed = started.elapsed();

        assert_eq!(sent, hits);
        println!(
            "hits {hits}  result {} bytes  per hit {:.2} us",
            cached.len(),
            elapsed.as_secs_f64() * 1e6 / hits as f64
        );
    }

    #[test]
    fn skipped_envelope_carries_solana_error_code() {
        let resp = serve_skipped_envelope(json!(42));
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
