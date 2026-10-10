//! The request path of a public instance
//!
//! Meter the caller, gate the method, then answer from the read cache or
//! upstream. See `docs/public-rpc.md`.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde_json::{Value, json};
use tracing::debug;

use tape_forwarded::caller::resolve_caller_ip;
use tape_forwarded::cidr::CidrBlock;

use crate::cache::MethodKind;
use crate::config::PublicConfig;
use crate::gate::{Gate, Refusal};
use crate::key::CacheKey;
use crate::meter::{Decision, Meter, Rate};
use crate::server::{AppState, raw_reply, reshape_with_id, serve_cached_read, upstream_err};
use crate::submit_log;
use crate::upstream::{RateLimitWait, RpcEnvelope, UpstreamError};

/// A public request waits out this much upstream cool-off before answering
///
/// Short, so a throttled caller hears back and retries on its own clock.
const PUBLIC_RATE_LIMIT_WAIT: RateLimitWait = RateLimitWait::Bounded(Duration::from_secs(2));

/// The error code public devnet sends with a 429, which clients already expect
const RATE_LIMITED_CODE: i64 = 429;
/// The message public devnet sends with a 429
const RATE_LIMITED_MESSAGE: &str = "Too many requests";

/// Waits shorter than this go out without `Retry-After`
///
/// solana-client then retries after 500 ms, which beats rounding a short
/// wait up to a whole second.
const RETRY_AFTER_FLOOR: Duration = Duration::from_secs(1);

/// A public instance always sends these reads upstream
///
/// A shared blockhash turns two identical sends into one transaction, which
/// the chain runs once, and a stale status delays every confirmation the CLI
/// waits on. The upstream bucket bounds what they cost.
const UNCACHED_METHODS: &[&str] = &["getLatestBlockhash", "getSignatureStatuses"];

/// A public instance reads request bodies up to this size
///
/// A v1 transaction is about 5.5 KiB of base64, and 256 signatures about 23 KiB.
pub const PUBLIC_BODY_LIMIT: usize = 64 * 1024;

/// What a public instance checks before serving a request
pub struct Public {
    pub gate: Gate,
    pub meter: Meter,
    pub trusted_proxies: Vec<CidrBlock>,
}

impl Public {
    /// The gate and buckets for these limits
    pub fn new(config: &PublicConfig) -> Self {
        let caller_rate = Rate {
            per_second: config.caller_per_second,
            burst: config.caller_burst,
        };
        let upstream_rate = Rate {
            per_second: config.upstream_per_second,
            burst: config.upstream_burst,
        };
        Self {
            gate: Gate::new(),
            meter: Meter::new(caller_rate, upstream_rate),
            trusted_proxies: config.trusted_proxies.clone(),
        }
    }
}

/// One JSON-RPC request from the public
struct Call {
    req: Value,
    method: String,
    params: Value,
    id: Value,
}

/// Answer one public request
pub async fn serve_public(
    state: &Arc<AppState>,
    public: &Public,
    peer: IpAddr,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    let caller = resolve_caller_ip(peer, headers, &public.trusted_proxies);
    if let Decision::Limited { retry_after } = public.meter.charge_caller(caller) {
        state.stats.public_callers_limited.fetch_add(1, Ordering::Relaxed);
        return rate_limited(Value::Null, retry_after);
    }

    let req = match parse_request(body) {
        Ok(req) => req,
        Err(refusal) => return refuse(state, caller, "", Value::Null, refusal),
    };
    let call = Call {
        method: req.get("method").and_then(Value::as_str).unwrap_or("").to_string(),
        params: req.get("params").cloned().unwrap_or(Value::Null),
        id: req.get("id").cloned().unwrap_or(Value::Null),
        req,
    };
    if let Err(refusal) = public.gate.check_request(&call.method, &call.params) {
        return refuse(state, caller, &call.method, call.id, refusal);
    }

    match state.policy.classify(&call.method) {
        MethodKind::Submit => submit(state, public, caller, call).await,
        MethodKind::Read { ttl } => read(state, public, caller, call, ttl).await,
        MethodKind::Unknown => refuse(state, caller, &call.method, call.id, Refusal::Method),
    }
}

/// Serve a read from the cache, or from upstream when the shared bucket allows
async fn read(
    state: &Arc<AppState>,
    public: &Public,
    caller: IpAddr,
    call: Call,
    ttl: Duration,
) -> Response {
    let is_cached = !UNCACHED_METHODS.contains(&call.method.as_str());
    let key = CacheKey::from_request(&call.method, &call.params);
    if is_cached {
        if let Some(cached) = state.cache.get(&key).await {
            return serve_cached_read(call.id, &cached);
        }
    }
    if let Decision::Limited { retry_after } = public.meter.charge_upstream() {
        state.stats.public_upstream_limited.fetch_add(1, Ordering::Relaxed);
        return rate_limited(call.id, retry_after);
    }

    let envelope = match forward(state, &call).await {
        Ok(envelope) => envelope,
        Err(error) => return upstream_failure(state, call.id, &error),
    };
    if let Some(result) = &envelope.result {
        if call.method == "getTransaction" {
            if let Err(refusal) = public.gate.check_transaction_result(result) {
                return refuse(state, caller, &call.method, call.id, refusal);
            }
        }
        // A null is a transaction that has not landed yet; caching it would
        // hide the landing from a CLI that polls until it does.
        if is_cached && !result.is_null() {
            if let Ok(serialized) = serde_json::to_vec(result) {
                state.cache.insert(key, Bytes::from(serialized), ttl).await;
            }
        }
    }
    (StatusCode::OK, Json(reshape_with_id(envelope, call.id))).into_response()
}

/// Relay a gated submit upstream when the shared bucket allows
async fn submit(
    state: &Arc<AppState>,
    public: &Public,
    caller: IpAddr,
    call: Call,
) -> Response {
    if state.log_submits {
        submit_log::record(&caller.to_string(), &call.method, &call.params);
    }
    if let Decision::Limited { retry_after } = public.meter.charge_upstream() {
        state.stats.public_upstream_limited.fetch_add(1, Ordering::Relaxed);
        return rate_limited(call.id, retry_after);
    }
    match forward(state, &call).await {
        Ok(envelope) => raw_reply(envelope),
        Err(error) => upstream_failure(state, call.id, &error),
    }
}

/// Send a request upstream
async fn forward(state: &AppState, call: &Call) -> Result<RpcEnvelope, UpstreamError> {
    state.stats.upstream_calls.fetch_add(1, Ordering::Relaxed);
    state.upstream.forward(&call.req, PUBLIC_RATE_LIMIT_WAIT).await
}

/// Answer a failed upstream call
///
/// An upstream cool-off longer than a public request waits becomes a 429
/// carrying the time left, so the caller waits it out and tries again.
fn upstream_failure(state: &AppState, id: Value, error: &UpstreamError) -> Response {
    match error {
        UpstreamError::RateLimited { .. } => {
            state.stats.public_upstream_limited.fetch_add(1, Ordering::Relaxed);
            rate_limited(id, state.upstream.cool_off_remaining())
        }
        UpstreamError::Http(_)
        | UpstreamError::Status { .. }
        | UpstreamError::BadResponse(_)
        | UpstreamError::GaveUp { .. }
        | UpstreamError::Closed => upstream_err(id, error).into_response(),
    }
}

/// One JSON-RPC request object; batches and other shapes are refused
fn parse_request(body: &[u8]) -> Result<Value, Refusal> {
    let req: Value = serde_json::from_slice(body).map_err(|_| Refusal::Malformed)?;
    match req {
        Value::Object(_) => Ok(req),
        Value::Array(_) => Err(Refusal::Batch),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Err(Refusal::Malformed),
    }
}

/// Answer a refused request with its JSON-RPC error
fn refuse(state: &AppState, caller: IpAddr, method: &str, id: Value, refusal: Refusal) -> Response {
    state.stats.public_refused.fetch_add(1, Ordering::Relaxed);
    debug!(%caller, %method, ?refusal, "refused public request");
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": refusal.code(),
            "message": refusal.message(),
        },
    });
    (StatusCode::OK, Json(body)).into_response()
}

/// Answer a 429, with `Retry-After` when the wait is a second or more
fn rate_limited(id: Value, retry_after: Duration) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": RATE_LIMITED_CODE,
            "message": RATE_LIMITED_MESSAGE,
        },
    });
    let mut response = (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
    if retry_after >= RETRY_AFTER_FLOOR {
        let seconds = retry_after.as_secs_f64().ceil() as u64;
        response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    // a single object parses; batches and scalars are refused
    #[test]
    fn request_shapes() {
        assert!(parse_request(br#"{"method":"getGenesisHash"}"#).is_ok());
        assert_eq!(parse_request(br#"[{"method":"getGenesisHash"}]"#), Err(Refusal::Batch));
        assert_eq!(parse_request(b"42"), Err(Refusal::Malformed));
        assert_eq!(parse_request(b"not json"), Err(Refusal::Malformed));
    }

    // short waits leave Retry-After off, long waits round up to whole seconds
    #[test]
    fn retry_after() {
        let short = rate_limited(Value::Null, Duration::from_millis(300));
        let long = rate_limited(Value::Null, Duration::from_millis(1_200));

        assert_eq!(short.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(short.headers().get(header::RETRY_AFTER).is_none());
        assert_eq!(long.headers().get(header::RETRY_AFTER), Some(&HeaderValue::from(2u64)));
    }
}
