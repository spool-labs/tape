use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::IntoResponse;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::spooler::VirtualGroup;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{with_share_token, BINARY_CONTENT, GetSliceReq};
use tape_sdk::bootstrap::counts_against_peer;
use tracing::debug;

use super::{parse_address, track_data_with_pending, track_with_pending};
use crate::cache::{CacheRead, CacheSource};
use crate::http::error::RouteError;
use crate::http::handlers::share::authorize_signed_read;
use crate::http::state::AppState;

pub async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, SpoolIndex)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    // Authorize ahead of the slice cache, whose entries are shared across callers.
    let token = authorize_signed_read(&state, &headers, &method, uri.path(), &track)?;

    let read = with_share_token(token, read_cached_slice(&state, track_addr, track.tape, spool_id)).await?;
    let data = read.data;
    state.context.metrics.add_downloaded(data.len() as u64);
    if read.source == CacheSource::Hit {
        debug!(track = %track_addr, spool = spool_id.0, bytes = data.len(), "gateway served cached slice");
    }

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], data))
}

pub async fn read_cached_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    tape: Address,
    spool_id: SpoolIndex,
) -> Result<CacheRead, RouteError> {
    state
        .slice_cache
        .get_or_insert_with(spool_id, track_addr, tape, || {
            fetch_slice_from_owner(state, track_addr, spool_id)
        })
        .await
}

/// Read one slice through the cache for a track whose record, blob and owner the caller already resolved
pub async fn read_resolved_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    origin: &SliceOrigin,
    spool_id: SpoolIndex,
    owner: Address,
) -> Result<CacheRead, RouteError> {
    state
        .slice_cache
        .get_or_insert_with(spool_id, origin.track_addr, origin.tape, || {
            fetch_verified_slice(state, origin, spool_id, owner)
        })
        .await
}

/// Slice fetches for one track check their bytes against these fields
#[derive(Clone, Copy, Debug)]
pub struct SliceOrigin {
    pub track_addr: Address,
    pub tape: Address,
    pub group: VirtualGroup,
    pub blob: BlobEncoding,
}

async fn fetch_slice_from_owner<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool_id: SpoolIndex,
) -> Result<Bytes, RouteError> {
    let track = track_with_pending(state, track_addr)?.ok_or(RouteError::NotFound)?;
    if !track.is_coded() {
        return Err(RouteError::BadRequest("track is not coded".into()));
    }

    let data = track_data_with_pending(state, track_addr, track.tape)?.ok_or(RouteError::NotFound)?;
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

    let origin = SliceOrigin {
        track_addr,
        tape: track.tape,
        group: track.group,
        blob,
    };
    fetch_verified_slice(state, &origin, spool_id, owner).await
}

async fn fetch_verified_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    origin: &SliceOrigin,
    spool_id: SpoolIndex,
    owner: Address,
) -> Result<Bytes, RouteError> {
    let position = origin
        .group
        .position_of(spool_id)
        .ok_or_else(|| RouteError::BadRequest("spool is not in the track's group".into()))?;

    let fetched = fetch_slice_from(state, origin, spool_id, owner).await;
    let data = match (fetched, previous_holder(state, spool_id, owner)) {
        (Ok(data), _) => data,
        (Err(_), Some(previous)) => fetch_slice_from(state, origin, spool_id, previous).await?,
        (Err(error), None) => return Err(error),
    };

    if !origin.blob.verify_slice(SpoolIndex(position as u64), &data) {
        return Err(RouteError::BadGateway("slice leaf hash mismatch".into()));
    }

    debug!(
        track = %origin.track_addr,
        spool = spool_id.0,
        owner = %owner,
        bytes = data.len(),
        "gateway fetched slice from owner"
    );

    Ok(data)
}

/// The node that held a stored spool last epoch, when today's holder is another node
///
/// It keeps serving the spool while the new holder pulls it, which after the
/// group count drops is every spool of a retired group.
pub fn previous_holder<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    spool_id: SpoolIndex,
    owner: Address,
) -> Option<Address> {
    state
        .context
        .state()
        .spool_owner_prev(spool_id)
        .filter(|held| *held != owner)
}

/// Ask one node for a slice and record how it answered
async fn fetch_slice_from<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    origin: &SliceOrigin,
    spool_id: SpoolIndex,
    node: Address,
) -> Result<Bytes, RouteError> {
    let started = Instant::now();
    let request = GetSliceReq {
        track: origin.track_addr,
        spool: spool_id,
    };

    match state.context.api.get_slice(node, &request).await {
        Ok(response) => {
            state.reputation.record_success(node, started.elapsed());
            Ok(response.data)
        }
        Err(error) => {
            if counts_against_peer(&error) {
                state.reputation.record_failure(node);
            }
            Err(RouteError::BadGateway(format!("get_slice: {error}")))
        }
    }
}
