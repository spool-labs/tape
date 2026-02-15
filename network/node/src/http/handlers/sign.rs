//! BLS signature handlers.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use store::Store;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::cert::track::CertifyMessage;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_node_api::{BlsSignResponse, BINARY_CONTENT};
use tape_store::ops::{MetaOps, TrackOps};
use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/sign — BLS sign track certification.
pub async fn get_signature<S: Store>(
    State(state): State<AppState<S>>,
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
        .get_current_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .unwrap_or(EpochNumber(0));

    let root = track_info.commitment_root();
    let msg = CertifyMessage::new(epoch, track_address.0, root.into());
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let (node_id, member_index) = state.context.committee_identity();
    let resp = BlsSignResponse {
        signature: sig.0 .0,
        node_id,
        member_index,
        epoch: epoch.0,
    };

    let bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

/// GET /v1/snapshots/:epoch/:chunk_index/sign — BLS sign snapshot chunk.
pub async fn get_snapshot_signature<S: Store>(
    State(state): State<AppState<S>>,
    Path((epoch, chunk_index)): Path<(u64, u64)>,
) -> Result<impl IntoResponse, ApiError> {
    let epoch = EpochNumber(epoch);
    let chunk_idx = ChunkIndex(chunk_index);

    let commitment = state
        .context
        .store
        .get_snapshot_commitment(epoch, chunk_idx)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let msg = SnapshotMessage::new(epoch, commitment.into());
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let (node_id, member_index) = state.context.committee_identity();
    let resp = BlsSignResponse {
        signature: sig.0 .0,
        node_id,
        member_index,
        epoch: epoch.0,
    };

    let bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}
