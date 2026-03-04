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
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_node_api::{BlsSignResponse, SnapshotSignatureSubmission, BINARY_CONTENT};
use tape_store::ops::{MetaOps, TrackOps};
use tape_store::types::SnapshotPartialSignature;

use crate::http::error::ApiError;
use crate::http::state::{require_chain_epoch, AppState};

/// GET /v1/tracks/:track_id/sign — BLS sign track certification.
pub async fn get_signature<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(track_id = %track_id, "http get_signature start");
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let epoch = require_chain_epoch(&state)?;

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
    tracing::trace!(
        track_id = %track_id,
        epoch = epoch.0,
        "http get_signature success"
    );

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
    tracing::trace!(
        epoch,
        chunk_index,
        payload_bytes = body.len(),
        "http post_snapshot_signature start"
    );
    let request: SnapshotSignatureSubmission =
        wincode::deserialize(&body).map_err(|e| ApiError::BadRequest(format!("signature request: {e}")))?;

    let epoch = EpochNumber(epoch);
    if request.epoch != epoch {
        return Err(ApiError::BadRequest("epoch mismatch".into()));
    }

    let group = SpoolGroup(chunk_index);
    let chunk_idx = ChunkIndex(chunk_index);

    let member_index = request.member_index as usize;
    let cs = state.context.chain_state.load();
    let validating_committee = cs.committee_for(epoch)
        .ok_or(ApiError::NotFound)?;
    if validating_committee.is_empty() {
        return Err(ApiError::NotFound);
    }

    if member_index >= validating_committee.len() {
        return Err(ApiError::BadRequest("unknown member index".into()));
    }

    let member = &validating_committee[member_index];

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
            group.0,
            SnapshotPartialSignature {
                member_index: request.member_index,
                signature: request.signature,
                epoch: epoch.0,
            },
        )
        .map_err(|e| ApiError::InternalError(format!("store signature: {e}")))?;
    tracing::trace!(
        epoch = epoch.0,
        chunk_index,
        member_index = request.member_index,
        "http post_snapshot_signature success"
    );

    Ok((StatusCode::OK, [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)]))
}
