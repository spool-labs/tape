//! Existence-check handlers for slices, metadata, and tracks.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use rpc::Rpc;
use store::Store;
use tape_core::erasure::spool_for_slice;
use tape_store::ops::{SliceOps, TrackOps};
use tape_store::types::Pubkey;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/slices/:slice_index/status
pub async fn slice_status<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((track_id, slice_index)): Path<(String, u16)>,
) -> Result<StatusCode, ApiError> {
    let track_address = parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let spool_id = spool_for_slice(track_info.spool_group, slice_index as usize);

    let exists = state
        .context
        .store
        .has_slice(spool_id, track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    if exists {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// GET /v1/tracks/:track_id/metadata/status
pub async fn metadata_status<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let track_address = parse_track_address(&track_id)?;

    let exists = state
        .context
        .store
        .has_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    if exists {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// GET /v1/tracks/:track_id/status — track lifecycle status.
pub async fn track_status<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(track_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let track_address = parse_track_address(&track_id)?;

    let exists = state
        .context
        .store
        .has_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    if exists {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

/// Parse a base58-encoded track address into a Pubkey.
pub(crate) fn parse_track_address(track_id: &str) -> Result<Pubkey, ApiError> {
    let sol_pubkey: solana_sdk::pubkey::Pubkey = track_id
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("invalid track address: {track_id}")))?;
    Ok(Pubkey(sol_pubkey.to_bytes()))
}

