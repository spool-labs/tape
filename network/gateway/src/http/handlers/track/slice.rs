use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use rpc::Rpc;
use store::Store;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::merkle::hash_leaf;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, GetSliceReq};
use tracing::debug;

use super::{parse_address, track_data_with_pending, track_with_pending};
use crate::cache::{CacheRead, CacheSource};
use crate::http::error::RouteError;
use crate::http::state::AppState;

pub(crate) async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, SpoolIndex)>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let read = read_cached_slice(&state, track_addr, spool_id).await?;
    let data = read.data;
    state.context.metrics.add_downloaded(data.len() as u64);
    if read.source == CacheSource::Hit {
        debug!(track = %track_addr, spool = spool_id.0, bytes = data.len(), "gateway served cached slice");
    }

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], data))
}

pub(crate) async fn read_cached_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool_id: SpoolIndex,
) -> Result<CacheRead, RouteError> {
    state
        .slice_cache
        .get_or_insert_with(spool_id, track_addr, || {
            fetch_slice_from_owner(state, track_addr, spool_id)
        })
        .await
}

async fn fetch_slice_from_owner<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool_id: SpoolIndex,
) -> Result<Vec<u8>, RouteError> {
    let track = track_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_coded() {
        return Err(RouteError::BadRequest("track is not coded".into()));
    }

    let position = track
        .group
        .position_of(spool_id)
        .ok_or_else(|| RouteError::BadRequest("spool is not in track group".into()))?;

    let data = track_data_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;
    let BlobData::Coded(blob) = data else {
        return Err(RouteError::BadRequest(
            "track data is not blob metadata".into(),
        ));
    };

    let owner = state
        .context
        .state()
        .group_peers(track.group)
        .into_iter()
        .find_map(|(spool, node)| (spool == spool_id).then_some(node))
        .ok_or_else(|| RouteError::BadGateway("spool owner not found".into()))?;

    let response = state
        .context
        .api
        .get_slice(
            owner,
            &GetSliceReq {
                track: track_addr,
                spool: spool_id,
            },
        )
        .await
        .map_err(|error| RouteError::BadGateway(format!("get_slice: {error}")))?;

    if position >= GROUP_SIZE || hash_leaf(&response.data) != blob.leaves[position] {
        return Err(RouteError::BadGateway("slice leaf hash mismatch".into()));
    }

    debug!(
        track = %track_addr,
        spool = spool_id.0,
        owner = %owner,
        bytes = response.data.len(),
        "gateway fetched slice from owner"
    );

    Ok(response.data)
}
