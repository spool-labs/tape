//! Existence-check handlers for slices, metadata, and tracks.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use rpc::Rpc;
use store::Store;
use tape_store::ops::{SliceOps, TrackOps};
use tape_store::types::Pubkey;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/tracks/:track_id/slices/:spool_id/status
/// `spool_id` is a global spool index (0..SPOOL_COUNT-1), not group-relative.
pub async fn slice_status<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
) -> Result<StatusCode, ApiError> {
    tracing::trace!(track_id = %track_id, spool_id, "http slice_status start");
    let track_address = parse_track_address(&track_id)?;

    state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let exists = state
        .context
        .store
        .has_slice(spool_id, track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(track_id = %track_id, spool_id, exists, "http slice_status result");

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
    tracing::trace!(track_id = %track_id, "http metadata_status start");
    let track_address = parse_track_address(&track_id)?;

    let exists = state
        .context
        .store
        .has_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(track_id = %track_id, exists, "http metadata_status result");

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
    tracing::trace!(track_id = %track_id, "http track_status start");
    let track_address = parse_track_address(&track_id)?;

    let exists = state
        .context
        .store
        .has_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(track_id = %track_id, exists, "http track_status result");

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
