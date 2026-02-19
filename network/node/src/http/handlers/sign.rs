//! BLS signature handlers.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::cert::track::CertifyMessage;
use tape_core::erasure::group_for_spool;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_node_api::{BlsSignResponse, SnapshotSignatureSubmission, BINARY_CONTENT};
use tape_store::ops::{CommitteeOps, MetaOps, TrackOps};
use tape_store::types::SnapshotPartialSignature;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/sign — BLS sign track certification.
pub async fn get_signature<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let epoch = state
        .context
        .store
        .get_chain_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .unwrap_or(EpochNumber(0));

    let root = track_info.commitment_root();
    let msg = CertifyMessage::new(epoch, track_address.0, root.into());
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let resp = BlsSignResponse {
        signature: sig,
        node_id: state.context.node_id(),
        epoch,
    };

    let bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

/// POST /v1/snapshots/:epoch/:chunk_index/partial_signature — accept partial BLS signatures.
pub async fn post_snapshot_signature<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((epoch, chunk_index)): Path<(u64, u64)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let request: SnapshotSignatureSubmission =
        wincode::deserialize(&body).map_err(|e| ApiError::BadRequest(format!("signature request: {e}")))?;

    let epoch = EpochNumber(epoch);
    if request.epoch != epoch {
        return Err(ApiError::BadRequest("epoch mismatch".into()));
    }

    let group = chunk_index;
    let chunk_idx = ChunkIndex(chunk_index);

    let committee = state
        .context
        .store
        .get_committee(epoch)
        .map_err(|e| ApiError::InternalError(format!("read committee: {e}")))?
        .ok_or(ApiError::NotFound)?;

    let member_index = request.member_index as usize;
    if member_index >= committee.len() {
        return Err(ApiError::BadRequest("unknown member index".into()));
    }

    let member = &committee[member_index];

    if !member
        .spools
        .iter()
        .any(|&spool| group_for_spool(spool) == group)
    {
        return Err(ApiError::NotInCommittee);
    }

    let commitment = state
        .context
        .store
        .get_snapshot_commitment(epoch, chunk_idx)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let message = SnapshotMessage::new(epoch, commitment.into()).to_bytes();
    if request
        .signature
        .verify_aggregate(message, &[member.bls_pubkey])
        .is_err()
    {
        return Err(ApiError::InvalidSignature);
    }

    state
        .context
        .store
        .set_snapshot_partial_signature(
            epoch,
            group,
            SnapshotPartialSignature {
                member_index: request.member_index,
                signature: request.signature,
                epoch: epoch.0,
            },
        )
        .map_err(|e| ApiError::InternalError(format!("store signature: {e}")))?;

    Ok((StatusCode::OK, [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)]))
}
