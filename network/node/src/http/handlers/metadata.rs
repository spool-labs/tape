//! Metadata retrieval handler.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_store::ops::TrackOps;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(track_id = %track_id, "http get_metadata start");
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let data =
        wincode::serialize(&track_info).map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(track_id = %track_id, size = data.len(), "http get_metadata success");

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            tape_protocol::api::BINARY_CONTENT,
        )],
        data,
    ))
}
