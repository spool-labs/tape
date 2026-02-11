//! BLS signature handler for snapshot certification.

use axum::{
    extract::{Path, State},
    Json,
};
use store::Store;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::erasure::{group_for_spool, SPOOL_GROUP_COUNT};
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_node_api::SignResponse;
use tracing::debug;

use tape_store::ops::MetaOps;

use crate::features::api::ApiError;

use super::ApiState;

/// GET /v1/snapshots/{epoch}/sign/{chunk_index}
///
/// BLS-sign a snapshot chunk certification message.
/// Verifies that we're in committee and have stored slices for this chunk's spool group,
/// then signs a SnapshotMessage binding (epoch, chunk_index, commitment_hash).
pub async fn get_snapshot_sign<S: Store>(
    State(state): State<ApiState<S>>,
    Path((epoch, chunk_index)): Path<(u64, u64)>,
) -> Result<Json<SignResponse>, ApiError> {
    if !state.control_plane.is_in_committee() {
        return Err(ApiError::Unauthorized);
    }

    if (chunk_index as usize) >= SPOOL_GROUP_COUNT {
        return Err(ApiError::InvalidSliceIndex);
    }

    let epoch = EpochNumber(epoch);
    let group = chunk_index;

    // Check we own spools in this group
    let our_group_spools: Vec<u16> = state
        .control_plane
        .get_our_spools()
        .into_iter()
        .filter(|&s| group_for_spool(s) == group)
        .collect();

    if our_group_spools.is_empty() {
        return Err(ApiError::IncompleteSliceData);
    }

    // Look up the stored commitment for this chunk
    let commitment_hash = state
        .service
        .store
        .get_snapshot_commitment(epoch, ChunkIndex(chunk_index))
        .map_err(|e| ApiError::Internal(format!("store error: {}", e)))?
        .ok_or(ApiError::IncompleteSliceData)?
        .0;

    let message = SnapshotMessage::new(epoch, ChunkIndex(chunk_index), commitment_hash);
    let message_bytes = message.to_bytes();

    let signature = state
        .bls_keypair
        .sign(&message_bytes)
        .map_err(|e| ApiError::Internal(format!("BLS signing failed: {:?}", e)))?;

    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system
        .committee
        .index_of(&node_id)
        .ok_or(ApiError::Unauthorized)?;

    debug!(
        epoch = epoch.as_u64(),
        chunk_index = chunk_index,
        node_id = node_id.as_u64(),
        "Signed snapshot chunk commitment"
    );

    Ok(Json(SignResponse {
        signature: (signature.0).0,
        node_id: node_id.as_u64(),
        member_index: member_index as u8,
        epoch: epoch.as_u64(),
    }))
}
