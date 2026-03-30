use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, SyncSliceEntry, SyncSlicesRequest, SyncSlicesResponse, SyncTrackEntry,
    SyncTracksRequest, SyncTracksResponse,
};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn sync_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
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
        .map(|(track, slice_data)| SyncSliceEntry {
            track_address: track.0,
            slice_data,
        })
        .collect();

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

    let limit = (request.limit as usize).clamp(1, 1000);
    let scan_batch = limit.max(64);
    let mut scan_cursor = request.cursor.map(StorePubkey::new);
    let mut entries = Vec::with_capacity(limit);
    let mut next_cursor = None;

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
            next_cursor = Some(track_address.0);

            if !track.spool_group.contains(request.spool_index) {
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
                track_address: track_address.0,
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

        scan_cursor = next_cursor.map(StorePubkey::new);
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
