use rpc::Rpc;
use store::Store;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{TrackDataOps, TrackOps};

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(crate) mod catalog;
pub(crate) mod slice;

pub(crate) fn track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .get_track(track_addr)
        .map_err(store_error)?;
    Ok(state.context.pending.apply_to_track(track_addr, in_store))
}

pub(crate) fn track_data_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<BlobData>, RouteError> {
    match state.context.pending.track_data(track_addr) {
        Some(data) => Ok(Some(data)),
        None => state
            .context
            .store
            .get_track_data(track_addr)
            .map_err(store_error),
    }
}

pub(crate) fn parse_address(value: &str, label: &str) -> Result<Address, RouteError> {
    value
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid {label}: {error}")))
}
