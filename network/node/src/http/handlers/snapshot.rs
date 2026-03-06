//! Snapshot endpoint handlers.

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_protocol::api::BINARY_CONTENT;
use tape_store::ops::MetaOps;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/snapshots/:epoch/commitments — return snapshot chunk commitments.
pub async fn get_commitments<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    Path(epoch): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(epoch, "http get_commitments start");
    let epoch = EpochNumber(epoch);
    let mut commitments = Vec::with_capacity(SPOOL_GROUP_COUNT);
    for group in 0..SPOOL_GROUP_COUNT {
        match state
            .context
            .store
            .get_snapshot_commitment(epoch, ChunkIndex(group as u64))
            .map_err(|e| ApiError::InternalError(e.to_string()))?
        {
            Some(hash) => commitments.push(hash),
            None => return Err(ApiError::NotFound),
        }
    }
    let body =
        wincode::serialize(&commitments).map_err(|e| ApiError::InternalError(e.to_string()))?;
    tracing::trace!(
        epoch = epoch.0,
        commitments = commitments.len(),
        "http get_commitments success"
    );
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        body,
    ))
}
