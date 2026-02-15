//! Repair (sub-chunk extraction) handler.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use store::Store;
use tape_node_api::{RepairRequest, BINARY_CONTENT};

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// POST /v1/tracks/:track_id/repair — extract sub-chunks for repair.
pub async fn post_repair<S: Store>(
    State(_state): State<AppState<S>>,
    Path(_track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let _request: RepairRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("repair request: {e}")))?;

    // Repair logic will be implemented with supervisor tasks.
    // Return empty response as stub.
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        Vec::<u8>::new(),
    ))
}
