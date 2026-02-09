//! Slice upload, download, and status handlers.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use store::Store;
use tape_crypto::merkle::hash_leaf;
use tape_node_api::SlicePayload;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState, SPOOL_COUNT};

/// PUT /v1/tracks/:track_id/slices/:slice_index
///
/// Deserialize SlicePayload, verify merkle leaf hash, store slice.
pub async fn put_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    if slice_index >= SPOOL_COUNT as u16 {
        return Err(ApiError::InvalidSliceIndex);
    }

    let spool_idx = slice_index;
    if !state.control_plane.owns_spool(spool_idx) {
        return Err(ApiError::NotResponsible);
    }

    let track_address = parse_track_id(&track_id)?;

    let payload = SlicePayload::from_bytes(&body)
        .map_err(|e| ApiError::InvalidBody(format!("SlicePayload: {}", e)))?;

    // Verify leaf hash matches data
    let computed_leaf = hash_leaf(&payload.data);
    if computed_leaf != payload.leaf_hash {
        return Err(ApiError::MerkleVerificationFailed);
    }

    // Store the slice data
    let data_len = payload.data.len();
    state
        .service
        .put_slice(spool_idx, track_address, payload.data)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    debug!(
        track = %track_address,
        slice_index,
        data_len,
        "stored slice"
    );

    state.metrics.slices_stored_total.inc();
    state.metrics.bytes_stored_total.add(data_len as i64);

    Ok(StatusCode::CREATED.into_response())
}

/// GET /v1/tracks/:track_id/slices/:slice_index
///
/// Return raw slice bytes.
pub async fn get_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    if slice_index >= SPOOL_COUNT as u16 {
        return Err(ApiError::InvalidSliceIndex);
    }

    let track_address = parse_track_id(&track_id)?;

    let data = state
        .service
        .get_slice(slice_index, track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    match data {
        Some(bytes) => {
            state.metrics.slices_retrieved_total.inc();
            state.metrics.bytes_retrieved_total.add(bytes.len() as i64);

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                bytes,
            )
                .into_response())
        }
        None => Err(ApiError::NotFound),
    }
}

/// GET /v1/tracks/:track_id/slices/:slice_index/status
///
/// Check if a slice exists without loading data.
pub async fn get_slice_status<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    if slice_index >= SPOOL_COUNT as u16 {
        return Err(ApiError::InvalidSliceIndex);
    }

    let track_address = parse_track_id(&track_id)?;

    let exists = state
        .service
        .has_slice(slice_index, track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?;

    Ok(Json(serde_json::json!({ "exists": exists })).into_response())
}
