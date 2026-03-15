use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use tape_crypto::merkle::{hash_leaf, verify_proof};
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SlicePayload};
use tape_store::ops::{SliceOps, TrackOps};
use tracing::{debug, trace};

use crate::features::http::error::RouteError;
use crate::features::http::helpers::{
    deserialize_body, ensure_spool_writable, parse_track_key, store_error,
};
use crate::features::http::state::AppState;

pub async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, spool_id, "http get_slice start");

    let (_, track_key) = parse_track_key(&track_id)?;

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

    let (_, track_key) = parse_track_key(&track_id)?;
    let payload: SlicePayload = deserialize_body(&body, "slice payload")?;

    let track_info = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    if hash_leaf(&payload.data) != payload.leaf_hash {
        return Err(RouteError::BadRequest("leaf hash mismatch".into()));
    }

    let leaf_pos = (spool_id as usize) % SPOOL_GROUP_SIZE;
    if !verify_proof(
        &payload.data,
        &track_info.commitment_root(),
        &payload.merkle_proof,
        leaf_pos as u64,
        COMMITMENT_TREE_HEIGHT,
    ) {
        return Err(RouteError::BadRequest("invalid merkle proof".into()));
    }

    ensure_spool_writable(&state, spool_id)?;

    state
        .context
        .store
        .put_slice(spool_id, track_key, payload.data)
        .map_err(store_error)?;

    debug!(track_id = %track_id, spool_id, "http put_slice success");
    Ok(StatusCode::OK)
}
