//! Spool synchronization handler.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_protocol::api::{SyncSpoolEntry, SyncSpoolRequest, SyncSpoolResponse, BINARY_CONTENT};
use tape_store::ops::{SliceOps, SpoolOps};
use tape_store::types::Pubkey;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// POST /v1/sync/spool — exchange spool data for sync.
pub async fn sync_spool<S: Store, R: Rpc>(
    State(state): State<AppState<S, R>>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(payload_bytes = body.len(), "http sync_spool start");
    let request: SyncSpoolRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("sync request: {e}")))?;
    tracing::trace!(
        spool = request.spool_index,
        cursor = ?request.cursor,
        limit = request.limit,
        "http sync_spool request parsed"
    );

    // Verify we own this spool
    let owned_spools = state
        .context
        .store
        .iter_all_spools()
        .map_err(|e| ApiError::InternalError(format!("read spools: {e}")))?;

    let owns_spool = owned_spools
        .iter()
        .any(|(id, _)| *id == request.spool_index);

    if !owns_spool {
        return Err(ApiError::NotResponsible);
    }

    let cursor = request.cursor.map(Pubkey::new);
    let limit = (request.limit as usize).min(1000);

    let slices = state
        .context
        .store
        .iter_slices_by_spool_from(request.spool_index, cursor, limit)
        .map_err(|e| ApiError::InternalError(format!("read slices: {e}")))?;

    let next_cursor = if slices.len() >= limit {
        slices.last().map(|(addr, _)| addr.0)
    } else {
        None
    };

    let entries: Vec<SyncSpoolEntry> = slices
        .into_iter()
        .map(|(addr, data)| SyncSpoolEntry {
            track_address: addr.0,
            slice_data: data,
        })
        .collect();

    let response = SyncSpoolResponse {
        entries,
        next_cursor,
    };
    tracing::trace!(
        spool = request.spool_index,
        response_entries = response.entries.len(),
        has_more = response.next_cursor.is_some(),
        "http sync_spool success"
    );

    let bytes = wincode::serialize(&response)
        .map_err(|e| ApiError::InternalError(format!("serialize response: {e}")))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}
