use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::track::data::BlobData;
use tape_core::track::types::{latest_track, CompressedTrack};
use tape_core::types::{SpoolIndex, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{TrackDataOps, TrackOps};

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(crate) mod catalog;
pub(crate) mod slice;

/// The track a read by address answers with, with pending state folded in.
///
/// Tracks written with the same content share an address. The latest certified
/// one answers, or the latest one when none is certified.
pub fn track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    Ok(latest_track(&tracks_with_pending(state, track_addr)?))
}

/// The track a slice read asks for.
///
/// Tracks under one address share their slices within a group, so any of them
/// whose group holds the spool answers.
pub fn spool_track_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    spool: SpoolIndex,
) -> Result<Option<CompressedTrack>, RouteError> {
    let mut tracks = tracks_with_pending(state, track_addr)?;
    tracks.retain(|info| info.group.contains(spool));
    Ok(latest_track(&tracks))
}

/// Every track under an address, lowest track number first, with pending state folded in
pub fn tracks_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
) -> Result<Vec<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .tracks_by_address(track_addr)
        .map_err(store_error)?;
    Ok(state.context.pending.apply_to_tracks(track_addr, in_store))
}

/// The track at one address and track number, with pending state folded in
pub fn track_at_with_pending<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track_number: TrackNumber,
) -> Result<Option<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .get_track(track_addr, track_number)
        .map_err(store_error)?;
    Ok(state.context.pending.apply_to_track(track_addr, track_number, in_store))
}

/// The track a request asks for: the one at its track number, or the track a
/// read by address answers with when it carries none
pub fn requested_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_addr: Address,
    track_number: Option<TrackNumber>,
) -> Result<Option<CompressedTrack>, RouteError> {
    match track_number {
        Some(track_number) => track_at_with_pending(state, track_addr, track_number),
        None => track_with_pending(state, track_addr),
    }
}

/// Address of the track at one number on a tape, from the store or the pending overlay
pub fn track_address_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    track_number: TrackNumber,
) -> Result<Option<Address>, RouteError> {
    let stored = state
        .context
        .store
        .track_address_by_number(tape, track_number)
        .map_err(store_error)?;
    Ok(stored.or_else(|| state.context.pending.track_address_by_number(tape, track_number)))
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
    track: &CompressedTrack,
) -> Result<Option<BlobData>, RouteError> {
    ensure_readable(state, track_addr, track.tape)?;
    match state.context.pending.track_data(track_addr, track.track_number) {
        Some(data) => Ok(Some(data)),
        None => state
            .context
            .store
            .get_track_data(track_addr, track.track_number)
            .map_err(store_error),
    }
}

pub fn parse_address(value: &str, label: &str) -> Result<Address, RouteError> {
    value
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid {label}: {error}")))
}
