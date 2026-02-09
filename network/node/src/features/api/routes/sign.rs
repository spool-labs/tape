//! BLS signature handlers for track certification.

use axum::{
    extract::{Path, State},
    Json,
};
use store::Store;
use tape_core::cert::track::CertifyMessage;
use tape_core::erasure::spool_in_group;
use tape_core::spooler::SpoolIndex;
use tape_node_api::SignResponse;
use tape_store::types::SpoolAllocation;
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

    // Filter owned spools to the track's spool group
    let group = match track_info.spool_allocation {
        SpoolAllocation::SpoolGroup(g) => g,
        SpoolAllocation::SpoolSingle(_) => {
            return Err(ApiError::Internal("single-spool tracks not supported for certification".into()));
        }
    };

    let our_group_spools: Vec<SpoolIndex> = state
        .control_plane
        .get_our_spools()
        .into_iter()
        .filter(|&s| spool_in_group(s, group))
        .collect();

    if our_group_spools.is_empty() {
        return Err(ApiError::IncompleteSliceData);
    }

    // Verify all our slices in this group exist in storage
    for spool_idx in &our_group_spools {
        state
            .service
            .get_slice(*spool_idx, track_address)
            .map_err(|e| ApiError::Storage(e.to_string()))?
            .ok_or(ApiError::IncompleteSliceData)?;
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
