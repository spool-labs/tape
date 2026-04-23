//! Axum server glue. Single `POST /` handler that parses the JSON-RPC
//! request body, classifies, and either serves from cache or forwards
//! upstream.
//!
//! Batch requests (top-level JSON array) pass through unchanged — a v1
//! simplification. Most Solana clients send singletons, so we lose
//! minimal caching opportunity.

use std::sync::Arc;

use axum::{
    Router,
    extract::{ConnectInfo, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::net::SocketAddr;
use tracing::{info, warn};

use crate::cache::{CacheStore, MethodKind, Policy};
use crate::key::CacheKey;
use crate::submit_log;
use crate::upstream::{Upstream, UpstreamError};

pub struct AppState {
    pub policy: Policy,
    pub cache: CacheStore,
    pub upstream: Upstream,
    pub log_submits: bool,
    /// Every request must include `?api=<api_key>` or it gets a 401. Not
    /// a security boundary — a cheap filter to keep port scanners from
    /// discovering an open Solana endpoint.
    pub api_key: String,
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
        .with_state(state)
}

async fn handle(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(auth): Query<AuthQuery>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Auth check first — reject anything without the expected key before
    // we spend work parsing the body. Deliberately terse so scanners
    // don't learn what shape is expected.
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

    // Batch: pass-through without caching.
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

    match state.policy.classify(method) {
        MethodKind::Submit => {
            if state.log_submits {
                submit_log::record(&caller, method, &params);
            }
            forward_raw(&state, &req).await
        }
        MethodKind::Read { ttl: _ttl } => {
            // Check cache first.
            let key = CacheKey::from_request(method, &params);
            if let Some(cached) = state.cache.get(&key).await {
                return json_ok(id, (*cached).clone()).into_response();
            }

            // Miss — forward and populate. moka's get_with would coalesce
            // concurrent misses, but we lean on the cache's own
            // concurrency: in the worst case two nodes duplicate one
            // upstream call, which is cheap compared to the uncached
            // baseline. Keeps the error path straightforward.
            match state.upstream.forward(&req).await {
                Ok(envelope) => {
                    if let Some(result) = &envelope.result {
                        state
                            .cache
                            .insert(key, Arc::new(result.clone()))
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

async fn forward_raw(state: &AppState, req: &Value) -> axum::response::Response {
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
