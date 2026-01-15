//! Health and status handlers.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_metrics::OperationTimer;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/status
pub async fn get_status<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Retrieve track info from storage
    match state.service.get_track_info(track_address) {
        Ok(Some(info)) => {
            let is_certified = info.certified_epoch.0 > 0;
            let mut response = serde_json::json!({
                "track_id": track_id,
                "slice_count": info.slice_count,
                "is_certified": is_certified
            });

            // Include certified_epoch only if certified
            if is_certified {
                response["certified_epoch"] = serde_json::json!(info.certified_epoch.0);
            }

            state
                .metrics
                .record_request("get_status", "success", timer.elapsed_secs());

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&response).unwrap_or_default(),
            )
                .into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_status", "not_found", timer.elapsed_secs());
            Err(ApiError::TrackNotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track status");
            state
                .metrics
                .record_request("get_status", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// GET /v1/health
pub async fn health_check() -> Response {
    StatusCode::OK.into_response()
}
