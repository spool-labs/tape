use std::fmt::Display;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::track::TrackWriteMessage;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse};
use tape_store::ops::{SliceOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn certify<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {

    let epoch = current_epoch(&state)?;
    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track_key = track;

    let in_store = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?;

    let track = state
        .context
        .pending
        .apply_to_track(track_key, in_store)
        .ok_or(RouteError::NotFound)?;

    if !track.is_blob() {
        return Err(RouteError::BadRequest(
            "raw tracks do not require certification".into(),
        ));
    }

    let protocol = state.context.state();
    let has_local_slice = protocol
        .group_peers(track.group)
        .into_iter()
        .filter(|(_, node)| *node == state.context.node_address())
        .any(|(spool_id, _)| {
            state
                .context
                .store
                .has_slice(spool_id, track_key)
                .unwrap_or(false)
        });

    if !has_local_slice {
        return Err(RouteError::NotFound);
    }

    let message = TrackWriteMessage::new(epoch, track.get_hash());

    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsSignResponse {
        signature,
        node: state.context.node_address(),
        epoch,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize certify response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
