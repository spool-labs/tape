//! Slice upload and download handlers.
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

use super::{parse_track_id, ApiState, SPOOL_COUNT};

/// GET /v1/tracks/:track_id/slices/:slice_index
pub async fn get_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    // Validate slice index
    if slice_index >= SPOOL_COUNT as u16 {
        return Err(ApiError::InvalidSliceIndex);
    }

    let track_address = parse_track_id(&track_id)?;

    debug!(
        track = %track_address,
        slice_index,
        "get_slice (stub)"
    );

    // Stub: return not found
    state.metrics.slices_retrieved_total.inc();
    Err(ApiError::NotFound)
}

/// PUT /v1/tracks/:track_id/slices/:slice_index
pub async fn put_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Validate slice index
    if slice_index >= SPOOL_COUNT as u16 {
        return Err(ApiError::InvalidSliceIndex);
    }

    // Verify spool ownership
    let spool_idx = slice_index;
    if !state.control_plane.owns_spool(spool_idx) {
        return Err(ApiError::NotResponsible);
    }

    let track_address = parse_track_id(&track_id)?;

    debug!(
        track = %track_address,
        slice_index,
        body_len = body.len(),
        "put_slice (stub)"
    );

    // Stub: accept but don't store
    state.metrics.slices_stored_total.inc();
    Ok(StatusCode::CREATED.into_response())
}
