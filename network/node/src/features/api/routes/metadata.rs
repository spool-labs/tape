//! Track metadata handlers.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use store::Store;
use tape_node_api::CONTENT_TYPE_WINCODE;
use tracing::debug;

use crate::features::api::ApiError;
use crate::features::storage::TrackInfo;

use super::{parse_track_id, ApiState};

/// PUT /v1/tracks/:track_id/metadata
///
/// Deserialize TrackInfo from wincode body and store it.
pub async fn put_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let info: TrackInfo = wincode::deserialize(&body)
        .map_err(|e| ApiError::InvalidBody(format!("TrackInfo: {}", e)))?;

    debug!(
        track = %track_address,
        original_size = info.original_size,
        "storing track metadata"
    );

    state
        .service
        .put_track(track_address, info)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    Ok(StatusCode::CREATED.into_response())
}

/// GET /v1/tracks/:track_id/metadata
///
/// Return TrackInfo as wincode bytes.
pub async fn get_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let info = state
        .service
        .get_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    match info {
        Some(track_info) => {
            let bytes = wincode::serialize(&track_info)
                .map_err(|e| ApiError::Serialization(e.to_string()))?;

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, CONTENT_TYPE_WINCODE)],
                bytes,
            )
                .into_response())
        }
        None => Err(ApiError::TrackNotFound),
    }
}

/// GET /v1/tracks/:track_id/metadata/status
///
/// Check if track metadata exists.
pub async fn get_metadata_status<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let exists = state
        .service
        .has_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    Ok(Json(serde_json::json!({ "exists": exists })).into_response())
}
