//! Structured logging of transaction submits. Goal is to surface the
//! worst offenders without throttling anything yet.

use serde_json::Value;
use tracing::info;

/// Record a submit. `caller` is typically the source IP of the node; we
/// keep it as a free-form string so callers that add an `X-Tape-Node-Id`
/// header can slot it in later.
pub fn record(caller: &str, method: &str, params: &Value) {
    let summary = summarize_params(method, params);
    info!(
        target: "rpc_cache::submit",
        %caller,
        %method,
        %summary,
        "tx submit"
    );
}

/// Best-effort extraction of high-signal bits from the params. Exhaustive
/// disassembly is out of scope; this just grabs the surface features an
/// operator eyeballing logs cares about.
fn summarize_params(method: &str, params: &Value) -> String {
    match method {
        "sendTransaction" | "sendAndConfirmTransaction" | "simulateTransaction" => {
            // First param is typically the base64-encoded tx. We report
            // its size as a proxy for tx complexity.
            if let Some(arr) = params.as_array() {
                if let Some(Value::String(b64)) = arr.first() {
                    return format!("tx_base64_len={}", b64.len());
                }
            }
            "tx=?".into()
        }
        _ => "(unsupported method)".into(),
    }
}
