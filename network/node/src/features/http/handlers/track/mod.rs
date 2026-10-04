pub mod catalog;
pub mod inconsistency;
pub mod repair;
pub mod sign;
pub mod slice;
pub mod sync;

use tape_core::track::types::{latest_track, CompressedTrack};
use tape_core::types::{SpoolIndex, TrackNumber};
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

/// Every track under an address, lowest track number first, with pending state folded in
pub(crate) fn tracks_by_address<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: Address,
) -> Result<Vec<CompressedTrack>, RouteError> {
    let in_store = state
        .context
        .store
        .tracks_by_address(track)
        .map_err(|error| RouteError::Internal(error.to_string()))?;

    Ok(state.context.pending.apply_to_tracks(track, in_store))
}

/// The one track at a number on a tape, with its address and pending state folded in.
///
/// A tape and a track number say where a track sits, which is how a request
/// that acts on one track identifies it.
pub(crate) fn resolve_tape_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
    track_number: TrackNumber,
) -> Result<Option<(Address, CompressedTrack)>, RouteError> {
    let stored = state
        .context
        .store
        .track_address_by_number(tape, track_number)
        .map_err(|error| RouteError::Internal(error.to_string()))?;
    let pending = || state.context.pending.track_address_by_number(tape, track_number);
    let Some(track) = stored.or_else(pending) else {
        return Ok(None);
    };

    let in_store = state
        .context
        .store
        .get_track(track, track_number)
        .map_err(|error| RouteError::Internal(error.to_string()))?;
    let found = state.context.pending.apply_to_track(track, track_number, in_store);

    Ok(found.map(|info| (track, info)))
}

/// The track a slice read asks for.
///
/// A spool holds one copy of a slice per address, so any track under the
/// address whose group holds the spool answers.
pub(crate) fn resolve_spool_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: Address,
    spool: SpoolIndex,
) -> Result<Option<CompressedTrack>, RouteError> {
    let mut tracks = tracks_by_address(state, track)?;
    tracks.retain(|info| info.group.contains(spool));
    Ok(latest_track(&tracks))
}

/// The track a data read asks for.
///
/// Tracks under one address hold the same data, and a node serves it for the
/// groups it is in, so any such track answers.
pub(crate) fn resolve_owned_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track: Address,
) -> Result<Option<CompressedTrack>, RouteError> {
    let protocol = state.context.state();
    let node = state.context.node_address();
    let is_owner = |group| protocol.group_peers(group).into_iter().any(|(_, peer)| peer == node);

    let tracks = tracks_by_address(state, track)?;
    let owned: Vec<CompressedTrack> = tracks.iter().copied().filter(|info| is_owner(info.group)).collect();
    // A node in none of the groups still answers for the latest track, so the
    // caller learns from that track's group that it asked the wrong node.
    Ok(latest_track(&owned).or_else(|| latest_track(&tracks)))
}
