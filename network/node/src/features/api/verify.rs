//! Independent inconsistency verification and BLS attestation.

use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::{Path, State},
    Json,
};
use store::Store;
use tape_core::cert::invalidate::InvalidateMessage;
use tape_core::erasure::{group_start, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolIndex;
use tape_node_api::{InconsistencyRequest, InconsistencyResponse};
use tape_node_client::NodeClientBuilder;
use tape_store::ops::CommitteeOps;
use tracing::debug;

use super::{parse_track_id, ApiError, ApiState};
use crate::features::recovery::decode::download_and_reencode;

/// Number of concurrent slice downloads during inconsistency verification.
const VERIFY_DOWNLOAD_CONCURRENCY: usize = 8;

/// POST /v1/tracks/:track_id/inconsistency
///
/// Independently verify an inconsistency and return a BLS attestation.
///
/// The handler performs full recovery (download k slices, decode, re-encode)
/// to independently verify that the erasure-coded commitment doesn't match.
/// Only signs if independent computation agrees with the requester's claim.
pub async fn post_inconsistency<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Json<InconsistencyResponse>, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let request: InconsistencyRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::InvalidBody(format!("InconsistencyRequest: {}", e)))?;

    // Must be in committee to attest
    if !state.control_plane.is_in_committee() {
        return Err(ApiError::Unauthorized);
    }

    // Load track metadata
    let track_info = state
        .service
        .get_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::TrackNotFound)?;

    let on_chain_root = track_info.commitment_root();

    // Quick reject: no inconsistency if computed_root matches on-chain
    if request.computed_root == on_chain_root {
        return Err(ApiError::InvalidBody(
            "computed_root matches on-chain commitment".into(),
        ));
    }

    // Independent verification: download k slices, decode, re-encode, compute root
    let computed_root = independently_verify(&state, track_address, &track_info).await?;

    // Our computed root must match the requester's claim
    if computed_root != request.computed_root {
        return Err(ApiError::InvalidBody(
            "independent verification produced different root".into(),
        ));
    }

    // Our computed root must differ from on-chain (confirms inconsistency)
    if computed_root == on_chain_root {
        return Err(ApiError::Internal(
            "independent verification matches on-chain commitment".into(),
        ));
    }

    // Build and sign the invalidation message
    let epoch = state.control_plane.current_epoch();
    let invalidate_message = InvalidateMessage::new(
        epoch,
        track_address.to_bytes(),
        computed_root.0,
    );
    let message = invalidate_message.to_bytes();

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
        "signed inconsistency attestation"
    );

    Ok(Json(InconsistencyResponse {
        signature: (signature.0).0,
        node_id: node_id.as_u64(),
        member_index: member_index as u8,
        epoch: epoch.as_u64(),
    }))
}

/// Download k slices, decode, re-encode, and return the computed merkle root.
async fn independently_verify<S: Store>(
    state: &ApiState<S>,
    track_address: tape_crypto::Pubkey,
    track_info: &tape_store::types::TrackInfo,
) -> Result<tape_crypto::Hash, ApiError> {
    let spool_group = track_info.spool_group;
    let start = group_start(spool_group);

    // Resolve group members from locally-cached committee
    let epoch = state.control_plane.current_epoch();
    let committee = state
        .service
        .store
        .get_committee(epoch)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::Internal("no committee available".into()))?;

    let mut spool_to_node: HashMap<SpoolIndex, usize> = HashMap::new();
    for (idx, member) in committee.iter().enumerate() {
        for &spool in &member.spools {
            spool_to_node.insert(spool, idx);
        }
    }

    let mut available = Vec::new();
    for position in 0..SPOOL_GROUP_SIZE {
        let spool_idx = start + position as SpoolIndex;
        if let Some(&member_idx) = spool_to_node.get(&spool_idx) {
            let member = &committee[member_idx];
            let addr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };
            match NodeClientBuilder::new()
                .accept_invalid_certs(state.insecure)
                .build(&addr.to_string())
            {
                Ok(client) => available.push((position, spool_idx, client)),
                Err(_) => continue,
            }
        }
    }

    let track_id_str = track_address.to_string();
    let (computed_root, _all_slices) =
        download_and_reencode(available, track_info, &track_id_str, VERIFY_DOWNLOAD_CONCURRENCY)
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(computed_root)
}
