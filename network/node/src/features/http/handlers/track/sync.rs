use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::system::BlacklistEntry;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, SyncSliceEntry, SyncSlicesRequest, SyncSlicesResponse, SyncTrackEntry,
    SyncTracksRequest, SyncTracksResponse,
};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};

use crate::features::blacklist::blacklist_entries_for_node;
use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

const MAX_SYNC_BATCH: usize = 1000;
const MIN_SCAN_BATCH: usize = 64;

pub async fn sync_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncSlicesRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("sync request: {error}")))?;
    state
        .context
        .store
        .get_spool_state(request.spool_index)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let limit = (request.limit as usize).clamp(1, MAX_SYNC_BATCH);
    // A page boundary rather than a row, so a peer resuming may be handed
    // slices it already holds; it skips what it has rather than rewriting it.
    let (slices, next_cursor) = state
        .context
        .store
        .sweep_slices_by_spool(request.spool_index, request.cursor.as_deref(), limit)
        .map_err(store_error)?;

    let current_epoch = state.context.state().epoch();

    // The blacklist is the same set for every row, and reading it per row read
    // the whole thing per row. The track metadata comes back in one call too.
    let refused = blacklist_entries_for_node(
        state.context.store.as_ref(),
        state.context.node_address(),
        current_epoch,
    )
    .map_err(store_error)?;
    let mut addresses: Vec<Address> = Vec::with_capacity(slices.len());
    for (track_address, _) in &slices {
        addresses.push(*track_address);
    }
    let metadata = state
        .context
        .store
        .get_tracks(&addresses)
        .map_err(store_error)?;

    let mut entries = Vec::with_capacity(slices.len());
    for ((track_address, slice_data), held) in slices.into_iter().zip(metadata) {
        let Some(track) = held else {
            continue;
        };

        if refused.contains(&BlacklistEntry::track(track_address))
            || refused.contains(&BlacklistEntry::tape(track.tape))
        {
            continue;
        }

        entries.push(SyncSliceEntry {
            track_address: track_address.to_bytes(),
            slice_data,
        });
    }

    let response = SyncSlicesResponse {
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

pub async fn sync_tracks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncTracksRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("sync tracks request: {error}")))?;
    state
        .context
        .store
        .get_spool_state(request.spool_index)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let limit = (request.limit as usize).clamp(1, MAX_SYNC_BATCH);
    let scan_batch = limit.max(MIN_SCAN_BATCH);
    let mut scan_cursor = request.cursor;
    let mut entries = Vec::with_capacity(limit);
    let mut next_cursor = None;
    let current_epoch = state.context.state().epoch();

    // One read of the blacklist for the whole scan rather than one per track.
    let refused = blacklist_entries_for_node(
        state.context.store.as_ref(),
        state.context.node_address(),
        current_epoch,
    )
    .map_err(store_error)?;

    loop {
        // A page boundary rather than a row: the mark the sweep answers names
        // where the next page starts, so a peer resuming from it may be handed
        // rows it already took. Taking a track twice is taking it once.
        let (tracks, next) = state
            .context
            .store
            .sweep_tracks(scan_cursor.as_deref(), scan_batch)
            .map_err(store_error)?;
        next_cursor = next.clone();

        for (track_address, track) in tracks.iter() {
            if !track.group.contains(request.spool_index) {
                continue;
            }

            if refused.contains(&BlacklistEntry::track(*track_address))
                || refused.contains(&BlacklistEntry::tape(track.tape))
            {
                continue;
            }

            let Some(data) = state
                .context
                .store
                .get_track_data(*track_address)
                .map_err(store_error)?
            else {
                continue;
            };

            entries.push(SyncTrackEntry {
                track_address: track_address.to_bytes(),
                data,
            });

            if entries.len() == limit {
                let response = SyncTracksResponse {
                    entries,
                    next_cursor,
                };

                let bytes = wincode::serialize(&response).map_err(|error| {
                    RouteError::Internal(format!("serialize sync tracks response: {error}"))
                })?;

                return Ok((
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, BINARY_CONTENT)],
                    bytes,
                ));
            }
        }

        match next {
            Some(next) => scan_cursor = Some(next),
            None => break,
        }
    }

    let response = SyncTracksResponse {
        entries,
        next_cursor,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize sync tracks response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
