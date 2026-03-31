use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_crypto::Pubkey;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, RepairRequest};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tape_store::types::TrackData;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;
use crate::features::spool::repair::extract_repair_data;

pub async fn repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("repair request: {error}")))?;

    let track: Pubkey = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    let track_key = track.into();

    state
        .context
        .store
        .get_spool_state(request.helper_spool)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let track = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    if !track.is_blob() {
        return Err(RouteError::BadRequest("raw tracks do not support repair".into()));
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

    let helper_slice = state
        .context
        .store
        .get_slice(request.helper_spool, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let output = extract_repair_data(
        &blob,
        &request.stripes, 
        &helper_slice
    ).map_err(|error| RouteError::BadRequest(error.to_string()))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        output,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
