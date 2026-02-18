//! Slice upload and retrieval handlers.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_core::cert::track::CertifyMessage;
use tape_core::erasure::{spool_for_slice, COMMITMENT_TREE_HEIGHT};
use tape_core::types::EpochNumber;
use tape_crypto::merkle::{hash_leaf, verify_proof};
use tape_crypto::ed25519::{PublicKey, Signature};
use tape_node_api::{BlsSignResponse, SignedMessage, SlicePayload, BINARY_CONTENT};
use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};

use crate::fsm::UserEvent;
use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/slices/:slice_index
pub async fn get_slice<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let spool_id = spool_for_slice(track_info.spool_group, slice_index as usize);

    let data = state
        .context
        .store
        .get_slice(spool_id, track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;
    state.context.stats.add_downloaded(data.len() as u64);

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        data,
    ))
}

/// PUT /v1/tracks/:track_id/slices/:slice_index — public (authority-signed) upload.
pub async fn put_slice<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let signed: SignedMessage = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("signed message: {e}")))?;

    let payload: SlicePayload = wincode::deserialize(&signed.message)
        .map_err(|e| ApiError::BadRequest(format!("payload: {e}")))?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    // Verify leaf hash
    let computed_leaf = hash_leaf(&payload.data);
    if computed_leaf != payload.leaf_hash {
        return Err(ApiError::BadRequest("leaf hash mismatch".into()));
    }

    // Verify merkle proof
    let root = track_info.commitment_root();
    if !verify_proof(
        &payload.data,
        &root,
        &payload.merkle_proof,
        slice_index as u64,
        COMMITMENT_TREE_HEIGHT,
    ) {
        return Err(ApiError::BadRequest("invalid merkle proof".into()));
    }

    // Verify Ed25519 signature over the message bytes
    let pubkey = PublicKey::from_bytes(signed.pubkey)
        .map_err(|_| ApiError::InvalidSignature)?;
    let sig = Signature::from_bytes(signed.signature)
        .map_err(|_| ApiError::InvalidSignature)?;
    sig.verify(&signed.message, &pubkey)
        .map_err(|_| ApiError::InvalidSignature)?;

    // Check spool ownership
    let spool_id = spool_for_slice(track_info.spool_group, slice_index as usize);
    verify_spool_ownership(&state, spool_id)?;

    // Store the slice
    let data_len = payload.data.len() as u64;
    state
        .context
        .store
        .put_slice(spool_id, track_address, payload.data)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;
    state.context.stats.add_uploaded(data_len);

    // Notify FSM of accepted slice
    if let Some(tx) = &state.user_event_tx {
        if tx.try_send(UserEvent::SliceAccepted {
            track: track_address.into(),
            spool: spool_id,
        }).is_err() {
            tracing::warn!(spool = spool_id, "user event channel full or closed");
        }
    }

    // BLS sign a CertifyMessage
    let epoch = state.context.store.get_current_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .unwrap_or(EpochNumber(0));

    let msg = CertifyMessage::new(epoch, track_address.0, root.into());
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let node_id = state.context.node_id();
    let resp = BlsSignResponse {
        signature: sig.0 .0,
        node_id,
        epoch: epoch.0,
    };

    let resp_bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        resp_bytes,
    ))
}

/// PUT /v1/internal/tracks/:track_id/slices/:slice_index — internal (peer) upload.
pub async fn put_slice_internal<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let payload: SlicePayload = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("payload: {e}")))?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    // Verify leaf hash
    let computed_leaf = hash_leaf(&payload.data);
    if computed_leaf != payload.leaf_hash {
        return Err(ApiError::BadRequest("leaf hash mismatch".into()));
    }

    // Verify merkle proof
    let root = track_info.commitment_root();
    if !verify_proof(
        &payload.data,
        &root,
        &payload.merkle_proof,
        slice_index as u64,
        COMMITMENT_TREE_HEIGHT,
    ) {
        return Err(ApiError::BadRequest("invalid merkle proof".into()));
    }

    // Check spool ownership
    let spool_id = spool_for_slice(track_info.spool_group, slice_index as usize);
    verify_spool_ownership(&state, spool_id)?;

    // Store the slice
    let data_len = payload.data.len() as u64;
    state
        .context
        .store
        .put_slice(spool_id, track_address, payload.data)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;
    state.context.stats.add_uploaded(data_len);

    // Notify FSM of accepted slice
    if let Some(tx) = &state.user_event_tx {
        if tx.try_send(UserEvent::SliceAccepted {
            track: track_address.into(),
            spool: spool_id,
        }).is_err() {
            tracing::warn!(spool = spool_id, "user event channel full or closed");
        }
    }

    // BLS sign a CertifyMessage
    let epoch = state.context.store.get_current_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .unwrap_or(EpochNumber(0));

    let msg = CertifyMessage::new(epoch, track_address.0, root.into());
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let node_id = state.context.node_id();
    let resp = BlsSignResponse {
        signature: sig.0 .0,
        node_id,
        epoch: epoch.0,
    };

    let resp_bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        resp_bytes,
    ))
}

/// Verify the node owns the given spool.
fn verify_spool_ownership<S: Store, R: Rpc>(
    state: &AppState<S, R>,
    spool_id: u16,
) -> Result<(), ApiError> {
    let spools = state
        .context
        .store
        .iter_all_spools()
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    if spools.iter().any(|(id, _)| *id == spool_id) {
        Ok(())
    } else {
        Err(ApiError::NotResponsible)
    }
}
