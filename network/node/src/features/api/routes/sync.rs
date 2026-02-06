//! Spool synchronization handlers.

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tracing::debug;

use crate::features::api::ApiError;

use super::ApiState;

/// POST /v1/migrate/sync_spool
///
/// Node-to-node spool synchronization endpoint.
/// Accepts a wincode-encoded sync request and returns slice data for the requested spool.
///
/// TODO: Define SyncSpoolRequest/Response types and implement full protocol.
pub async fn sync_spool<S: Store>(
    State(_state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    debug!(
        body_len = body.len(),
        "sync_spool"
    );

    // Stub: return empty response until sync protocol types are defined
    let response = serde_json::json!({
        "version": "v1",
        "slices": []
    });

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&response).unwrap_or_default(),
    )
        .into_response())
}
