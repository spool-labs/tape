//! Metadata upload and retrieval handlers.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_store::ops::TrackOps;
use tape_store::types::TrackInfo;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let data =
        wincode::serialize(&track_info).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            tape_node_api::BINARY_CONTENT,
        )],
        data,
    ))
}

/// PUT /v1/tracks/:track_id/metadata — public upload.
pub async fn put_metadata<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info: TrackInfo = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("metadata: {e}")))?;

    state
        .context
        .store
        .put_track(track_address, track_info)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok(StatusCode::OK)
}

/// PUT /v1/internal/tracks/:track_id/metadata — internal (peer) upload.
pub async fn put_metadata_internal<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info: TrackInfo = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("metadata: {e}")))?;

    state
        .context
        .store
        .put_track(track_address, track_info)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok(StatusCode::OK)
}
