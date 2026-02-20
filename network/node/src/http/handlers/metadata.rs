//! Metadata upload and retrieval handlers.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_crypto::{merkle::root_from_leaf_hashes, Hash};
use tape_store::ops::TrackOps;
use tape_store::types::TrackInfo;
use tape_core::erasure::COMMITMENT_TREE_HEIGHT;

use crate::http::error::ApiError;
use crate::http::state::AppState;

const MAX_TRACK_COMMITMENT_LEAVES: usize = 1 << COMMITMENT_TREE_HEIGHT;

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store, R: Rpc>(
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

    let data =
        wincode::serialize(&track_info).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            tape_node_api::BINARY_CONTENT,
        )],
        data,
    ))
}

/// PUT /v1/tracks/:track_id/metadata — public upload.
pub async fn put_metadata<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    put_metadata_inner(&state, track_id, body).await
}

/// PUT /v1/internal/tracks/:track_id/metadata — internal (peer) upload.
pub async fn put_metadata_internal<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    put_metadata_inner(&state, track_id, body).await
}

async fn put_metadata_inner<S: Store, R: Rpc>(
    state: &AppState<S, R>,
    track_id: String,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info: TrackInfo = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("metadata: {e}")))?;

    let expected = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    verify_metadata_match(&track_info, &expected)?;

    state
        .context
        .store
        .put_track(track_address, track_info)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok(StatusCode::OK)
}

fn verify_metadata_match(incoming: &TrackInfo, existing: &TrackInfo) -> Result<(), ApiError> {
    if incoming.tape_address != existing.tape_address {
        return Err(ApiError::BadRequest("metadata mismatch: tape address".into()));
    }

    if incoming.spool_group != existing.spool_group {
        return Err(ApiError::BadRequest("metadata mismatch: spool group".into()));
    }

    if incoming.original_size != existing.original_size {
        return Err(ApiError::BadRequest("metadata mismatch: original size".into()));
    }

    if incoming.stripe_size != existing.stripe_size {
        return Err(ApiError::BadRequest("metadata mismatch: stripe size".into()));
    }

    if incoming.stripe_count != existing.stripe_count {
        return Err(ApiError::BadRequest("metadata mismatch: stripe count".into()));
    }

    if incoming.encoding_type != existing.encoding_type {
        return Err(ApiError::BadRequest("metadata mismatch: encoding type".into()));
    }

    if incoming.encoding_params != existing.encoding_params {
        return Err(ApiError::BadRequest("metadata mismatch: encoding params".into()));
    }

    let existing_root = metadata_commitment_root(existing)?;
    let incoming_root = metadata_commitment_root(incoming)?;
    if incoming_root != existing_root {
        return Err(ApiError::BadRequest("metadata mismatch: commitment root".into()));
    }

    Ok(())
}

fn metadata_commitment_root(track_info: &TrackInfo) -> Result<Hash, ApiError> {
    if track_info.commitment.len() > MAX_TRACK_COMMITMENT_LEAVES {
        return Err(ApiError::BadRequest("metadata mismatch: invalid commitment length".into()));
    }

    Ok(root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&track_info.commitment))
}
