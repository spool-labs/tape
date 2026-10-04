use std::fmt::Display;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::types::TrackNumber;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse};
use tape_store::ops::SliceOps;

use crate::features::blacklist::refuses_object;
use crate::features::http::error::RouteError;
use crate::features::http::handlers::track::{parse_tape, resolve_tape_track};
use crate::features::http::state::{AppState, current_epoch};

pub async fn certify<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number)): Path<(String, u64)>,
) -> Result<impl IntoResponse, RouteError> {

    let epoch = current_epoch(&state)?;
    let tape = parse_tape(&tape_id)?;

    let (track_key, track) = resolve_tape_track(&state, tape, TrackNumber(track_number))?
        .ok_or(RouteError::NotFound)?;

    if !track.is_coded() {
        return Err(RouteError::BadRequest(
            "raw tracks do not require certification".into(),
        ));
    }

    if refuses_object(
        state.context.store.as_ref(),
        &state.context.blacklist,
        state.context.node_address(),
        epoch,
        track_key,
        track.tape,
    )
    .map_err(store_error)?
    {
        return Err(RouteError::BlacklistedObject);
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

    let signature = state
        .context
        .sign_track_write(epoch, track.get_hash())
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
