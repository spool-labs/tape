use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SyncSpoolEntry, SyncSpoolRequest, SyncSpoolResponse};
use tape_store::ops::{SliceOps, SpoolOps};
use tape_store::types::Pubkey as StorePubkey;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn sync_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncSpoolRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("sync request: {error}")))?;
    state
        .context
        .store
        .get_spool_state(request.spool_index)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let cursor = request.cursor.map(StorePubkey::new);
    let limit = (request.limit as usize).clamp(1, 1000);
    let slices = state
        .context
        .store
        .iter_slices_by_spool_from(request.spool_index, cursor, limit)
        .map_err(store_error)?;

    let next_cursor = if slices.len() == limit {
        slices.last().map(|(track, _)| track.0)
    } else {
        None
    };

    let entries = slices
        .into_iter()
        .map(|(track, slice_data)| SyncSpoolEntry {
            track_address: track.0,
            slice_data,
        })
        .collect();

    let response = SyncSpoolResponse {
        entries,
        next_cursor,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize sync response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
