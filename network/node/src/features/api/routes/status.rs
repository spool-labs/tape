//! Health and status handlers.
//!
//! NOTE: get_status is currently a stub pending API redesign.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use store::Store;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/status
pub async fn get_status<S: Store>(
    State(_state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    debug!(
        track = %track_address,
        "get_status (stub)"
    );

    // Stub: return not found
    Err(ApiError::TrackNotFound)
}

/// GET /v1/health
pub async fn health_check() -> Response {
    StatusCode::OK.into_response()
}
