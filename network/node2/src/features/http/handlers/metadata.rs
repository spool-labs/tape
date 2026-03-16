use std::fmt::Display;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_crypto::Pubkey;
use tape_protocol::Api;
use tape_protocol::api::BINARY_CONTENT;
use tape_store::ops::TrackOps;
use tape_store::types::Pubkey as StorePubkey;
use tracing::trace;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn get_metadata<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, "http get_metadata start");

    let track: Pubkey = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    let track_key: StorePubkey = track.into();
    let track_info = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let bytes = wincode::serialize(&track_info)
        .map_err(|error| RouteError::Internal(format!("serialize metadata: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
