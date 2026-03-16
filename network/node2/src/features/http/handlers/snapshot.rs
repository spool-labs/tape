use std::fmt::Display;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_protocol::Api;
use tape_protocol::api::BINARY_CONTENT;
use tape_store::ops::MetaOps;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn get_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(epoch): Path<u64>,
) -> Result<impl IntoResponse, RouteError> {
    let epoch = EpochNumber(epoch);
    let mut commitments = Vec::with_capacity(SPOOL_GROUP_COUNT);

    for group in 0..SPOOL_GROUP_COUNT {
        let commitment = state
            .context
            .store
            .get_snapshot_commitment(epoch, ChunkIndex(group as u64))
            .map_err(store_error)?
            .ok_or(RouteError::NotFound)?;
        commitments.push(commitment);
    }

    let bytes = wincode::serialize(&commitments).map_err(|error| {
        RouteError::Internal(format!("serialize snapshot commitments: {error}"))
    })?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
