//! BLS signature handlers for track certification.

use axum::{
    extract::{Path, State},
    Json,
};
use store::Store;
use tape_core::cert::track::CertifyMessage;
use tape_crypto::merkle::hash_leaf;
use tape_node_api::SignResponse;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/sign
///
/// BLS-sign the track certification message.
/// Verifies that all slices owned by this node match their commitment leaves,
/// then signs a CertifyMessage binding (epoch, track_address, commitment_hash).
pub async fn get_sign<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Json<SignResponse>, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    if !state.control_plane.is_in_committee() {
        return Err(ApiError::Unauthorized);
    }

    let track_info = state
        .service
        .get_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::TrackNotFound)?;

    if track_info.commitment.is_empty() {
        return Err(ApiError::IncompleteSliceData);
    }

    // Verify each slice we own matches its commitment leaf
    for spool_idx in state.control_plane.get_our_spools() {
        let data = state
            .service
            .get_slice(spool_idx, track_address)
            .map_err(|e| ApiError::Storage(e.to_string()))?
            .ok_or(ApiError::IncompleteSliceData)?;

        let computed_leaf = hash_leaf(&data);
        if let Some(expected_leaf) = track_info.commitment.get(spool_idx as usize) {
            if computed_leaf != *expected_leaf {
                return Err(ApiError::MerkleVerificationFailed);
            }
        }
    }

    let epoch = state.control_plane.current_epoch();

    // Build CertifyMessage with domain separation, epoch binding, and commitment binding
    let certify_message = CertifyMessage::new(
        epoch,
        track_address.to_bytes(),
        track_info.commitment_hash.0,
    );
    let message = certify_message.to_bytes();

    let signature = state
        .bls_keypair
        .sign(&message)
        .map_err(|e| ApiError::Internal(format!("BLS signing failed: {:?}", e)))?;

    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system
        .committee
        .index_of(&node_id)
        .ok_or(ApiError::Unauthorized)?;

    debug!(
        track = %track_address,
        node_id = node_id.as_u64(),
        "signed track commitment"
    );

    Ok(Json(SignResponse {
        signature: (signature.0).0,
        node_id: node_id.as_u64(),
        member_index: member_index as u8,
        epoch: epoch.as_u64(),
    }))
}
