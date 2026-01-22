//! Spool synchronization handlers.
//!
//! NOTE: This handler is currently a stub pending API redesign.

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
pub async fn sync_spool<S: Store>(
    State(_state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    debug!(
        body_len = body.len(),
        "sync_spool (stub)"
    );

    // Stub: return empty response
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
