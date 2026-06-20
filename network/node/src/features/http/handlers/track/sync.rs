use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, SyncSliceEntry, SyncSlicesRequest, SyncSlicesResponse, SyncTrackEntry,
    SyncTracksRequest, SyncTracksResponse,
};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};

use crate::features::blacklist::refuses_object;
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

    let cursor = request.cursor.map(Address::new);
    let limit = (request.limit as usize).clamp(1, MAX_SYNC_BATCH);
    let slices = state
        .context
        .store
        .iter_slices_by_spool_from(request.spool_index, cursor, limit)
        .map_err(store_error)?;

    let next_cursor = if slices.len() == limit {
        slices.last().map(|(track, _)| track.to_bytes())
    } else {
        None
    };

    let current_epoch = state.context.state().epoch();
    let mut entries = Vec::with_capacity(slices.len());
    for (track_address, slice_data) in slices {
        let Some(track) = state
            .context
            .store
            .get_track(track_address)
            .map_err(store_error)?
        else {
            continue;
        };

        if refuses_object(
            state.context.store.as_ref(),
            state.context.node_address(),
            current_epoch,
            track_address,
            track.tape,
        )
        .map_err(store_error)?
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
    let mut scan_cursor = request.cursor.map(Address::new);
    let mut entries = Vec::with_capacity(limit);
    let mut next_cursor = None;
    let current_epoch = state.context.state().epoch();

    loop {
        let tracks = state
            .context
            .store
            .iter_tracks_from(scan_cursor, scan_batch)
            .map_err(store_error)?;

        if tracks.is_empty() {
            next_cursor = None;
            break;
        }

        for (track_address, track) in tracks.iter() {
            next_cursor = Some(track_address.to_bytes());

            if !track.group.contains(request.spool_index) {
                continue;
            }

            if refuses_object(
                state.context.store.as_ref(),
                state.context.node_address(),
                current_epoch,
                *track_address,
                track.tape,
            )
            .map_err(store_error)?
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

        if tracks.len() < scan_batch {
            next_cursor = None;
            break;
        }

        scan_cursor = next_cursor.map(Address::new);
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
