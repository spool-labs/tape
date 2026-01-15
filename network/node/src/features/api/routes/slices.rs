//! Slice upload and download handlers.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use store::Store;
use tape_metrics::OperationTimer;
use tape_node_api::SlicePayload;

use crate::features::api::ApiError;
use crate::features::storage::{Compression, SliceMeta, MERKLE_HEIGHT};
use tape_crypto::merkle::verify_proof;

use super::{parse_track_id, ApiState, MAX_SLICE_SIZE, SLICE_COUNT};

/// GET /v1/tracks/:track_id/slices/:slice_index
pub async fn get_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("get_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // spool_idx == slice_index (always - by definition)
    let spool_idx = slice_index;

    // Retrieve from storage
    match state.service.get_slice(spool_idx, track_address) {
        Ok(Some((data, _meta))) => {
            state
                .metrics
                .record_request("get_slice", "success", timer.elapsed_secs());
            Ok((StatusCode::OK, data).into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_slice", "not_found", timer.elapsed_secs());
            Err(ApiError::NotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get slice");
            state
                .metrics
                .record_request("get_slice", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// PUT /v1/tracks/:track_id/slices/:slice_index
pub async fn put_slice<S: Store>(
    State(state): State<ApiState<S>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // Verify spool ownership - only accept slices for spools we're assigned to
    let spool_idx = slice_index; // slice_index == spool_index by definition
    if !state.control_plane.owns_spool(spool_idx) {
        state
            .metrics
            .record_request("put_slice", "not_responsible", timer.elapsed_secs());
        return Err(ApiError::NotResponsible);
    }

    // Parse track_id to Pubkey (needed early for metadata lookup)
    let track_address = parse_track_id(&track_id)?;

    // Deserialize SlicePayload from wincode
    let payload = SlicePayload::from_bytes(&body).map_err(|e| {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid slice payload: {}", e))
    })?;

    // Validate data size
    if payload.data.len() > MAX_SLICE_SIZE {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidBody("slice too large".into()));
    }

    // If track metadata exists (commitment_hash uploaded), verify merkle proof
    if let Ok(Some(track_info)) = state.service.get_track_info(track_address) {
        // Verify the merkle proof against the stored commitment (merkle root)
        let is_valid = verify_proof(
            &payload.data,
            &track_info.commitment_hash,
            &payload.merkle_proof,
            spool_idx as u64,
            MERKLE_HEIGHT,
        );
        if !is_valid {
            state
                .metrics
                .record_request("put_slice", "merkle_failed", timer.elapsed_secs());
            return Err(ApiError::MerkleVerificationFailed);
        }
    }
    // Note: If no track metadata yet, verification happens at signing time

    // Build metadata from payload
    let meta = SliceMeta {
        len: payload.data.len() as u32,
        leaf_hash: payload.leaf_hash,
        merkle_proof: payload.merkle_proof,
        compression: Compression::None,
        received_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };

    // Store
    match state
        .service
        .put_slice(spool_idx, track_address, payload.data, meta)
    {
        Ok(()) => {
            state
                .metrics
                .record_request("put_slice", "success", timer.elapsed_secs());
            Ok(StatusCode::CREATED.into_response())
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to put slice");
            state
                .metrics
                .record_request("put_slice", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}
