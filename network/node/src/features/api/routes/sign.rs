//! BLS signature handlers for track certification.

use axum::{
    extract::{Path, State},
    Json,
};
use store::Store;
use tape_crypto::hash::hashv;
use tape_node_api::SignResponse;
use tracing::debug;

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/sign
///
/// BLS-sign the track commitment for certification.
/// Signs over the hash of all commitment entries, producing a deterministic
/// signature that all committee members can independently verify.
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

    // Build signing message from commitment hashes.
    // All nodes with the same TrackInfo will produce the same message.
    let parts: Vec<&[u8]> = track_info.commitment.iter().map(|h| h.as_ref()).collect();
    let message = hashv(&parts);

    let signature = state
        .bls_keypair
        .sign(message.as_ref())
        .map_err(|e| ApiError::Internal(format!("BLS signing failed: {:?}", e)))?;

    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system
        .committee
        .index_of(&node_id)
        .ok_or(ApiError::Unauthorized)?;
    let epoch = state.control_plane.current_epoch();

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
