//! Repair and inconsistency handlers.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_node_api::RepairRequest;
use tape_slicer::ClayCoder;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// POST /v1/tracks/:track_id/repair
///
/// Helper-side handler for bandwidth-optimal repair.
/// Reads the specified slice, extracts the requested sub-chunks per stripe,
/// and returns concatenated bytes.
pub async fn post_repair<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::InvalidBody(format!("RepairRequest: {}", e)))?;

    // Read our slice from local storage
    let slice_data = state
        .service
        .get_slice(request.helper_spool, track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    // Parse slice metadata suffix to determine chunk/sub-chunk sizes
    let metadata = tape_slicer::SliceMetadata::from_slice(&slice_data)
        .map_err(|e| ApiError::Internal(format!("slice metadata: {}", e)))?;

    let total_data_len = slice_data.len().saturating_sub(tape_slicer::SliceMetadata::SIZE);
    let blob_len = metadata.blob_len();
    let stripe_size = metadata.stripe_size();
    let num_stripes = if blob_len == 0 {
        1
    } else {
        (blob_len + stripe_size - 1) / stripe_size
    };

    if total_data_len == 0 || num_stripes == 0 {
        return Err(ApiError::Internal("invalid slice layout".into()));
    }

    let chunk_size = total_data_len / num_stripes;

    // Repair is only supported for Clay-encoded tracks
    let profile = metadata.profile();
    if !profile.is_clay() {
        return Err(ApiError::InvalidBody(
            "repair only supported for Clay encoding".into(),
        ));
    }

    // Compute alpha from the encoding profile's clay parameters
    let clay_params = profile.clay_params();
    let coder = ClayCoder::new(
        clay_params.n() as usize,
        clay_params.k() as usize,
        clay_params.d() as usize,
    );
    let alpha = coder.alpha();

    if alpha == 0 || chunk_size % alpha != 0 {
        return Err(ApiError::Internal(format!(
            "chunk_size ({chunk_size}) not divisible by alpha ({alpha})"
        )));
    }
    let sub_chunk_size = chunk_size / alpha;

    // Extract requested sub-chunks
    let mut out = Vec::new();

    for stripe_req in &request.stripes {
        let stripe_idx = stripe_req.stripe as usize;
        let chunk_offset = stripe_idx * chunk_size;
        let chunk_end = chunk_offset + chunk_size;

        if chunk_end > total_data_len {
            return Err(ApiError::Internal("stripe index out of bounds".into()));
        }

        let chunk = &slice_data[chunk_offset..chunk_end];

        for &sc_idx in &stripe_req.sub_chunks {
            let start = sc_idx as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            if end > chunk.len() {
                return Err(ApiError::Internal("sub-chunk index out of bounds".into()));
            }
            out.extend_from_slice(&chunk[start..end]);
        }
    }

    debug!(
        track = %track_address,
        helper_spool = request.helper_spool,
        lost_slice = request.lost_slice,
        response_bytes = out.len(),
        "extracted repair sub-chunks"
    );

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        out,
    )
        .into_response())
}

/// POST /v1/tracks/:track_id/inconsistency
///
/// Attest proof inconsistency. Not yet implemented.
pub async fn post_inconsistency<S: Store>(
    State(_state): State<ApiState<S>>,
    Path(_track_id): Path<String>,
    _body: Bytes,
) -> Result<Response, ApiError> {
    Ok(StatusCode::NOT_IMPLEMENTED.into_response())
}
