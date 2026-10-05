pub mod catalog;
pub mod inconsistency;
pub mod repair;
pub mod sign;
pub mod slice;
pub mod sync;

use tape_core::track::types::CompressedTrack;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_store::ops::TrackOps;
use tape_store_api::Store;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub(crate) fn parse_track(track_id: &str) -> Result<Address, RouteError> {
    track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))
}

pub(crate) fn parse_tape(tape_id: &str) -> Result<Address, RouteError> {
    tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))
}

/// The track at an address, with pending state folded in
pub(crate) fn resolve_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .get_track(track)
        .map_err(|error| RouteError::Internal(error.to_string()))?;

    Ok(state.context.pending.apply_to_track(track, in_store))
}

/// The track at a number on a tape, with pending state folded in
pub(crate) fn resolve_tape_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    track_number: TrackNumber,
) -> Result<Option<CompressedTrack>, RouteError> {
    let stored = state
        .context
        .store
        .track_address_by_number(tape, track_number)
        .map_err(|error| RouteError::Internal(error.to_string()))?;
    // A registration that is confirmed but not finalized is only in the pending overlay.
    let pending = || state.context.pending.track_address_by_number(tape, track_number);
    let Some(track) = stored.or_else(pending) else {
        return Ok(None);
    };

    resolve_track(state, track)
}
