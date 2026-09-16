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

pub fn track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
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

/// Refuse a track this gateway no longer serves: removed, or on an unlisted tape
pub fn ensure_readable<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    tape: Address,
) -> Result<(), RouteError> {
    match state.blacklists.denies(track_addr, tape) {
        true => Err(RouteError::Gone),
        false => Ok(()),
    }
}

/// A track's stored content, refused once reads of the track are denied
pub fn track_data_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    tape: Address,
) -> Result<Option<BlobData>, RouteError> {
    ensure_readable(state, track_addr, tape)?;
    match state.context.pending.track_data(track_addr) {
        Some(data) => Ok(Some(data)),
        None => state
            .context
            .store
            .get_track_data(track_addr)
            .map_err(store_error),
    }
}

pub fn parse_address(value: &str, label: &str) -> Result<Address, RouteError> {
    value
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid {label}: {error}")))
}
