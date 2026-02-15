//! Spool synchronization handler.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use store::Store;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// POST /v1/sync/spool — exchange spool data for sync.
///
/// Accepts and returns raw wincode bytes. The sync protocol is opaque at this
/// layer; the body is forwarded to the spool sync engine.
pub async fn sync_spool<S: Store>(
    State(_state): State<AppState<S>>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    // Sync protocol will be implemented when the supervisor tasks are fleshed out.
    // For now, echo the request as a stub.
    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            tape_node_api::BINARY_CONTENT,
        )],
        body.to_vec(),
    ))
}
