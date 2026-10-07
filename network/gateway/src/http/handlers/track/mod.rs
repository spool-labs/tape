use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{TapeOps, TrackDataOps, TrackOps, track_has_left};
use tape_store::types::TapeInfo;

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(crate) mod catalog;
pub(crate) mod slice;

/// The track at an address, from the store or the pending overlay. A track
/// that has left its tape reads as absent: a gateway stops serving a deleted
/// track at once, and the sweep takes this one's row at the next epoch.
pub fn track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .get_track(track_addr)
        .map_err(store_error)?;
    let Some(track) = state.context.pending.apply_to_track(track_addr, in_store) else {
        return Ok(None);
    };

    let tape_row = tape_with_pending(state, track.tape)?;
    if track_has_left(tape_row.as_ref(), &track) {
        return Ok(None);
    }
    Ok(Some(track))
}

/// The row of the tape at an address now, with a pending reservation, close,
/// rewind or renew folded in
pub fn tape_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
) -> Result<Option<TapeInfo>, RouteError> {
    let in_store = state.context.store.get_tape(tape).map_err(store_error)?;
    Ok(state.context.pending.apply_to_tape(tape, in_store))
}

/// Address of the track at one number on a tape, from the store or the pending overlay
pub fn track_address_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    track_number: TrackNumber,
) -> Result<Option<Address>, RouteError> {
    let Some(tape_row) = tape_with_pending(state, tape)? else {
        return Ok(None);
    };

    // A track numbered below the tree's start left when the tape was rewound.
    if track_number < tape_row.tree_start {
        return Ok(None);
    }

    let stored = state
        .context
        .store
        .track_address_by_number(tape, tape_row.id, track_number)
        .map_err(store_error)?;
    let pending = || {
        state
            .context
            .pending
            .track_address_by_number(tape, tape_row.id, track_number)
    };
    Ok(stored.or_else(pending))
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
