//! Inconsistency attestation handler.

use bytemuck::cast;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::bft::is_supermajority;
use tape_core::cert::InvalidateMessage;
use tape_core::erasure::{group_for_spool, SPOOL_GROUP_SIZE};
use tape_core::types::EpochNumber;
use tape_protocol::api::{BlsInconsistencyResponse, InconsistencyRequest, BINARY_CONTENT};
use tape_store::ops::TrackOps;

use crate::core::NodeContext;
use crate::http::error::ApiError;
use crate::http::state::{require_chain_epoch, AppState};

/// POST /v1/tracks/:track_id/inconsistency — attest data inconsistency.
pub async fn post_inconsistency<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(track_id = %track_id, payload_bytes = body.len(), "http post_inconsistency start");
    let track_address = super::status::parse_track_address(&track_id)?;

    let request: InconsistencyRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("inconsistency request: {e}")))?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    verify_local_root_mismatch(&track_info.commitment_root(), request.proof.observed_root)?;
    let epoch = require_chain_epoch(&state)?;
    verify_inconsistency_proof(
        &state.context,
        &request.proof,
        &track_info,
        track_address.0,
        epoch,
    )?;

    let msg = InvalidateMessage::new(
        epoch,
        track_address.0,
        request.proof.observed_root.into(),
    );
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let resp = BlsInconsistencyResponse {
        signature: sig,
        node_id: state.context.node_id(),
        epoch,
    };
    let bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(track_id = %track_id, epoch = epoch.0, "http post_inconsistency success");

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn verify_local_root_mismatch(
    local_root: &tape_crypto::Hash,
    observed_root: tape_crypto::Hash,
) -> Result<(), ApiError> {
    if *local_root == observed_root {
        return Err(ApiError::BadRequest("roots match, no inconsistency".into()));
    }
    Ok(())
}

fn verify_inconsistency_proof<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    proof: &tape_protocol::api::InconsistencyProof,
    track_info: &tape_store::types::TrackInfo,
    track_address: [u8; 32],
    epoch: EpochNumber,
) -> Result<(), ApiError> {
    let cs = context.chain_state.load();
    if epoch != cs.epoch {
        return Err(ApiError::BadRequest("committee missing".into()));
    }
    let committee = &cs.committee;
    if committee.is_empty() {
        return Err(ApiError::BadRequest("committee missing".into()));
    }

    let max_bitmap_bits = tape_protocol::api::COMMITTEE_BITMAP_BYTES * 8;
    if committee.len() > max_bitmap_bits {
        return Err(ApiError::BadRequest("committee exceeds supported bitmap size".into()));
    }

    let bitmap: CommitteeBitmap = cast(proof.committee_bitmap);
    let signer_indices = bitmap.indices(committee.len());
    if signer_indices.is_empty() {
        return Err(ApiError::BadRequest("inconsistency proof has no signers".into()));
    }

    let mut signer_weight = 0u64;
    let mut signer_pubkeys = Vec::with_capacity(signer_indices.len());
    for signer_index in signer_indices {
        let member = committee
            .get(signer_index)
            .ok_or(ApiError::BadRequest(
                "inconsistency bitmap has unknown signer".to_string(),
            ))?;

        signer_weight += member
            .spools
            .iter()
            .filter(|&&spool| group_for_spool(spool) == track_info.spool_group)
            .count() as u64;
        signer_pubkeys.push(member.bls_pubkey);
    }

    if !is_supermajority(signer_weight, SPOOL_GROUP_SIZE as u64) {
        return Err(ApiError::BadRequest(
            "inconsistency proof lacks quorum for spool group".into(),
        ));
    }

    let msg = InvalidateMessage::new(epoch, track_address, proof.observed_root.into());
    proof
        .signature
        .verify_aggregate(msg.to_bytes(), &signer_pubkeys)
        .map_err(|_| ApiError::InvalidSignature)?;

    Ok(())
}
