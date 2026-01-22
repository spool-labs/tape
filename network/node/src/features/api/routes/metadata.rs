//! Track metadata handlers.
//!
//! NOTE: These handlers are currently stubs pending API redesign.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use store::Store;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store>(
    State(_state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    debug!(
        track = %track_address,
        "get_metadata (stub)"
    );

    // Stub: return not found
    Err(ApiError::TrackNotFound)
}

/// PUT /v1/tracks/:track_id/metadata
pub async fn put_metadata<S: Store>(
    State(_state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    debug!(
        track = %track_address,
        body_len = body.len(),
        "put_metadata (stub)"
    );

    // Stub: accept but don't store
    Ok(StatusCode::CREATED.into_response())
}
