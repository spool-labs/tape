use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use tape_core::track::data::TrackData;
use tape_crypto::address::Address;
use tape_crypto::merkle::{hash_leaf, verify_proof};
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SlicePayload};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tracing::{debug, trace};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, spool_id, "http get_slice start");

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track_key = track;

    state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let data = state
        .context
        .store
        .get_slice(spool_id, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    state.context.metrics.add_downloaded(data.len() as u64);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        data,
    ))
}

pub async fn put_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
    body: Bytes,
) -> Result<StatusCode, RouteError> {
    trace!(
        track_id = %track_id,
        spool_id,
        payload_bytes = body.len(),
        "http put_slice start"
    );

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track_key = track;
    let payload: SlicePayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("slice payload: {error}")))?;

    let track = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    if !track.is_blob() {
        return Err(RouteError::BadRequest("raw tracks do not accept slices".into()));
    }

    let track_data = state
        .context
        .store
        .get_track_data(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    let TrackData::Blob(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    if hash_leaf(&payload.data) != payload.leaf_hash {
        return Err(RouteError::BadRequest("leaf hash mismatch".into()));
    }

    let leaf_pos = (spool_id as usize) % SPOOL_GROUP_SIZE;

    if !verify_proof(
        &payload.data,
        &blob.commitment,
        &payload.merkle_proof,
        leaf_pos as u64,
        COMMITMENT_TREE_HEIGHT,
    ) {
        return Err(RouteError::BadRequest("invalid merkle proof".into()));
    }

    let spool_state = state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;
    
    if spool_state.is_locked() {
        return Err(RouteError::NotResponsible);
    }

    let data_len = payload.data.len() as u64;
    state
        .context
        .store
        .put_slice(spool_id, track_key, payload.data)
        .map_err(store_error)?;
    state.context.metrics.add_uploaded(data_len);

    debug!(track_id = %track_id, spool_id, "http put_slice success");

    Ok(StatusCode::OK)
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
