use std::{net::SocketAddr, str::FromStr};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;

use crate::metrics::{record_metrics, run_metrics_server, Process};
use crate::store::run_refresh_store;

use super::store::{StoreError, TapeStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcMethod {
    GetHealth,
    GetTapeAddress,
    GetTapeNumber,
    GetSegment,
}

impl RpcMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            RpcMethod::GetHealth => "getHealth",
            RpcMethod::GetTapeAddress => "getTapeAddress",
            RpcMethod::GetTapeNumber => "getTapeNumber",
            RpcMethod::GetSegment => "getSegment",
        }
    }
}

impl FromStr for RpcMethod {
    type Err = RpcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "getHealth" => Ok(RpcMethod::GetHealth),
            "getTapeAddress" => Ok(RpcMethod::GetTapeAddress),
            "getTapeNumber" => Ok(RpcMethod::GetTapeNumber),
            "getSegment" => Ok(RpcMethod::GetSegment),
            _ => Err(RpcError {
                code: ErrorCode::MethodNotFound.code(),
                message: "method not found".into(),
            }),
        }
    }
}

#[repr(i64)]
#[derive(Copy, Clone)]
pub enum ErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    ServerError = -32000,
}

impl ErrorCode {
    pub fn code(self) -> i64 {
        self as i64
    }
}

#[derive(Deserialize)]
struct RpcRequest {
    method: String,
    params: Value,
    id: Option<Value>,
}

#[derive(Serialize)]
pub struct RpcError {
    code: i64,
    message: String,
}

impl RpcError{
    pub fn err_code(&self) -> i64 {
        self.code
    }
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
    id: Option<Value>,
}

// Helper: wrap a Result<Value, RpcError> into RpcResponse
fn make_response(
    id: Option<Value>,
    result: Result<Value, RpcError>,
) -> (StatusCode, Json<RpcResponse>) {
    let (res, err) = match result {
        Ok(val) => (Some(val), None),
        Err(e) => (None, Some(e)),
    };
    let resp = RpcResponse {
        jsonrpc: "2.0".into(),
        result: res,
        error: err,
        id,
    };
    (StatusCode::OK, Json(resp))
}

/// Retrieve the last‐persisted block height & drift.
///
/// Example invocation:
/// ```bash
/// curl -X POST http://127.0.0.1:3000/api \
///      -H 'Content-Type: application/json' \
///      -d '{"jsonrpc":"2.0","id":5,"method":"getHealth","params":{}}'
/// ```
pub fn rpc_get_health(store: &TapeStore, _params: &Value) -> Result<Value, RpcError> {
    let (last_processed_slot, drift) = store
        .get_health()
        .map_err(|e| RpcError {
            code: ErrorCode::ServerError.code(),
            message: e.to_string(),
        })?;
    Ok(json!({ "last_processed_slot": last_processed_slot, "drift": drift }))
}

/// Retrieve the pubkey (tape address) associated with a tape number.
///
/// Parameters:
/// - `tape_number`: The numeric ID of the tape.
///
/// Returns the base-58-encoded Solana pubkey.
///
/// Example invocation:
/// ```bash
/// curl -X POST http://127.0.0.1:3000/api \
///      -H 'Content-Type: application/json' \
///      -d '{"jsonrpc":"2.0","id":1,"method":"getTapeAddress","params":{"tape_number":42}}'
/// ```
pub fn rpc_get_tape_address(store: &TapeStore, params: &Value) -> Result<Value, RpcError> {
    let tn = params
        .get("tape_number")
        .and_then(Value::as_u64)
        .ok_or(RpcError {
            code: ErrorCode::InvalidParams.code(),
            message: "invalid or missing tape_number".into(),
        })?;

    store
        .get_tape_address(tn)
        .map(|pk| json!(pk.to_string()))
        .map_err(|e| match e {
            StoreError::TapeNotFound(n) => RpcError {
                code: ErrorCode::ServerError.code(),
                message: format!("tape {n} not found"),
            },
            other => RpcError {
                code: ErrorCode::ServerError.code(),
                message: other.to_string(),
            },
        })
}

/// Look up the numeric tape ID for a given pubkey (tape address).
///
/// Parameters:
/// - `tape_address`: Base-58-encoded Solana pubkey.
///
/// Returns the `u64` tape number.
///
/// Example invocation:
/// ```bash
/// curl -X POST http://127.0.0.1:3000/api \
///      -H 'Content-Type: application/json' \
///      -d '{"jsonrpc":"2.0","id":2,"method":"getTapeNumber","params":{"tape_address":"<PUBKEY>"}}'
/// ```
pub fn rpc_get_tape_number(store: &TapeStore, params: &Value) -> Result<Value, RpcError> {
    let addr = params
        .get("tape_address")
        .and_then(Value::as_str)
        .ok_or(RpcError {
            code: ErrorCode::InvalidParams.code(),
            message: "invalid or missing tape_address".into(),
        })?;

    let pk = Pubkey::from_str(addr).map_err(|e| RpcError {
        code: ErrorCode::InvalidParams.code(),
        message: format!("invalid pubkey: {e}"),
    })?;

    store
        .get_tape_number(&pk)
        .map(|num| json!(num))
        .map_err(|e| match e {
            StoreError::TapeNotFoundForAddress(_) => RpcError {
                code: ErrorCode::ServerError.code(),
                message: "tape not found for address".into(),
            },
            other => RpcError {
                code: ErrorCode::ServerError.code(),
                message: other.to_string(),
            },
        })
}

/// Fetch a single segment’s data by tape number and segment number.
///
/// Parameters:
/// - `tape_number`: Numeric ID of the tape.
/// - `segment_number`: Zero-based segment index.
///
/// Returns a Base64-encoded string of the raw bytes.
///
/// Example invocation:
/// ```bash
/// curl -X POST http://127.0.0.1:3000/api \
///      -H 'Content-Type: application/json' \
///      -d '{"jsonrpc":"2.0","id":3,"method":"getSegment","params":{"tape_address":<PUBKEY>,"segment_number":3}}'
/// ```
pub fn rpc_get_segment(store: &TapeStore, params: &Value) -> Result<Value, RpcError> {
    let addr = params
        .get("tape_address")
        .and_then(Value::as_str)
        .ok_or(RpcError {
            code: ErrorCode::InvalidParams.code(),
            message: "invalid or missing tape_address".into(),
        })?;

    let sn = params
        .get("segment_number")
        .and_then(Value::as_u64)
        .ok_or(RpcError {
            code: ErrorCode::InvalidParams.code(),
            message: "invalid or missing segment_number".into(),
        })?;

    let pk = Pubkey::from_str(addr).map_err(|e| RpcError {
        code: ErrorCode::InvalidParams.code(),
        message: format!("invalid pubkey: {e}"),
    })?;

    store
        .get_segment(&pk, sn)
        .map(|data| json!(base64::encode(data)))
        .map_err(|e| match e {
            StoreError::TapeNotFound(_) => RpcError {
                code: ErrorCode::ServerError.code(),
                message: "tape not found".into(),
            },
            StoreError::SegmentNotFound(_, num) => RpcError {
                code: ErrorCode::ServerError.code(),
                message: format!("segment {num} not found"),
            },
            other => RpcError {
                code: ErrorCode::ServerError.code(),
                message: other.to_string(),
            },
        })
}





async fn rpc_handler(
    State(store): State<Arc<TapeStore>>,
    Json(req): Json<RpcRequest>,
) -> impl IntoResponse {
    let id = req.id.clone();

    let rpc_method = match RpcMethod::from_str(req.method.as_str()) {
        Ok(m) => m,
        Err(err) => {
            return make_response(id, Err(err));
        }
    };

    let outcome = record_metrics(&rpc_method, || {
        match rpc_method {
            RpcMethod::GetHealth => rpc_get_health(&store, &req.params),
            RpcMethod::GetTapeAddress => rpc_get_tape_address(&store, &req.params),
            RpcMethod::GetTapeNumber => rpc_get_tape_number(&store, &req.params),
            RpcMethod::GetSegment => rpc_get_segment(&store, &req.params),
        }
    });

    make_response(id, outcome)
}




pub async fn web_loop(
    store: TapeStore,
    port: u16,
) -> anyhow::Result<()> {

    // Run metrics server 
    run_metrics_server(Process::Web)?;

    let store = Arc::new(store);

    run_refresh_store(&store);

    let app = Router::new()
        .route("/api", post(rpc_handler))
        .with_state(store);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(listener, app).await?;

    Ok(())
}
