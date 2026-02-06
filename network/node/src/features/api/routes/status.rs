//! Health and track status handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use store::Store;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/status
///
/// Track lifecycle status derived from available data:
/// - No TrackInfo -> "nonexistent"
/// - TrackInfo but no slices -> "registered"
/// - TrackInfo + at least one slice -> "stored"
pub async fn get_track_status<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let has_metadata = state
        .service
        .has_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    if !has_metadata {
        return Ok(Json(serde_json::json!({ "status": "nonexistent" })).into_response());
    }

    // Check if we have any slices for this track in any of our owned spools
    let our_spools = state.control_plane.get_our_spools();
    let mut has_slices = false;

    for spool in &our_spools {
        if state
            .service
            .has_slice(*spool, track_address)
            .map_err(|e| ApiError::Storage(e.to_string()))?
        {
            has_slices = true;
            break;
        }
    }

    let status = if has_slices { "stored" } else { "registered" };

    Ok(Json(serde_json::json!({ "status": status })).into_response())
}

/// GET /v1/health
pub async fn health_check() -> Response {
    StatusCode::OK.into_response()
}
