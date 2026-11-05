use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::time::Duration;

use tape_core::encoding::ErasureCode;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};
use tape_crypto::address::Address;

use crate::attempt::{AttemptFailure, Method};
use crate::history::PositionPeer;
use crate::limits::SpoolLimits;
use crate::peer::{PeerOutcome, PeerScores};
use crate::time::WallTime;
use crate::track::MissingTrack;

/// Tracks that go by one method to the same peers
#[derive(Clone, Debug)]
pub struct FetchReq {
    /// The method every track goes by
    pub method: Method,
    /// Our live spool, as the fold of `live_groups` places it
    pub spool: SpoolIndex,
    /// The live group count the fold had when the turn read it
    pub live_groups: u64,
    /// The peers asked, each at its position
    pub peers: Vec<PositionPeer>,
    /// The tracks asked for
    pub tracks: Vec<FetchTrack>,
    /// What the tracks count toward the fetch's bytes
    pub bytes: StorageUnits,
}

/// One track in a fetch, with what the turn read for it
#[derive(Clone, Debug)]
pub struct FetchTrack {
    /// The missing track the fetch is for
    pub missing_track: MissingTrack,
    /// The track as the store holds it
    pub track: CompressedTrack,
    /// A coded track's encoding, when the store holds it
    pub encoding: Option<BlobEncoding>,
    /// For a transfer, the epoch of the holder asked
    pub holder_epoch: EpochNumber,
}

/// What a finished fetch sends back
#[derive(Debug)]
pub struct FetchRes {
    /// The fetch_req it ran
    pub req: FetchReq,
    /// How each peer asked did
    pub peers: Vec<(Address, PeerOutcome)>,
    /// How each track went: got it, or why not
    pub tracks: Vec<(Address, Result<(), AttemptFailure>)>,
}

/// The fetches running now, and the tracks and bytes in them, by the
/// number each fetch runs under
#[derive(Debug)]
pub struct FetchPool<Number> {
    fetches: HashMap<Number, FetchReq>,
    tracks: HashSet<Address>,
    bytes: StorageUnits,
    recoveries: usize,
}

impl FetchReq {
    /// A fetch_req with no tracks yet
    pub fn new(
        method: Method,
        spool: SpoolIndex,
        live_groups: u64,
        peers: Vec<PositionPeer>,
    ) -> Self {
        Self {
            method,
            spool,
            live_groups,
            peers,
            tracks: Vec::new(),
            bytes: StorageUnits(0),
        }
    }

    /// Whether a track of `bytes` fits: an empty fetch_req takes any track
    pub fn can_add_track(
        &self,
        bytes: StorageUnits,
        limits: &SpoolLimits,
    ) -> bool {
        let total = self.bytes.saturating_add(bytes);
        self.tracks.is_empty() || total <= limits.fetch_bytes
    }

    /// Add a track and what it counts
    pub fn add_track(&mut self, track: FetchTrack, bytes: StorageUnits) {
        self.tracks.push(track);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// Whether the fetch_req holds all it may: one track for a repair, since
    /// a repair request carries one, else the most tracks or bytes
    pub fn is_full(&self, limits: &SpoolLimits) -> bool {
        if self.method == Method::Repair {
            return !self.tracks.is_empty();
        }
        let is_at_tracks = self.tracks.len() >= limits.fetch_tracks;
        is_at_tracks || self.bytes >= limits.fetch_bytes
    }

    /// The tracks' addresses, in the fetch's order
    pub fn get_addresses(&self) -> Vec<Address> {
        let mut addresses = Vec::with_capacity(self.tracks.len());
        for track in &self.tracks {
            addresses.push(track.missing_track.track);
        }
        addresses
    }

    /// The fetch's track with this address
    pub fn get_track(&self, address: Address) -> Option<&FetchTrack> {
        let is_asked = |track: &&FetchTrack| {
            track.missing_track.track == address
        };
        self.tracks.iter().find(is_asked)
    }
}

impl FetchRes {
    /// A fetch_res with nothing in it yet
    pub fn new(req: FetchReq) -> Self {
        Self { req, peers: Vec::new(), tracks: Vec::new() }
    }

    /// Record how a peer did
    pub fn add_peer_result(&mut self, peer: Address, outcome: PeerOutcome) {
        self.peers.push((peer, outcome));
    }

    /// Record how a peer that answered did: it worked, unless it sent
    /// wrong data
    pub fn add_peer_answer(
        &mut self,
        peer: Address,
        bytes: StorageUnits,
        elapsed: Duration,
        is_wrong: bool,
    ) {
        let mut outcome = PeerOutcome::Worked { bytes, elapsed };
        if is_wrong {
            outcome = PeerOutcome::WrongData;
        }
        self.add_peer_result(peer, outcome);
    }

    /// Record how a track went
    pub fn add_track_result(
        &mut self,
        track: Address,
        outcome: Result<(), AttemptFailure>,
    ) {
        self.tracks.push((track, outcome));
    }

    /// Record that several tracks went the same way
    pub fn add_track_results(
        &mut self,
        tracks: &[Address],
        outcome: Result<(), AttemptFailure>,
    ) {
        for track in tracks {
            self.add_track_result(*track, outcome);
        }
    }

    /// Every track of the fetch failed the same way
    pub fn fail_every_track(&mut self, failure: AttemptFailure) {
        let tracks = self.req.get_addresses();
        self.add_track_results(&tracks, Err(failure));
    }

    /// A call to `peer` failed, so every track went with it
    pub fn add_failed_call(
        &mut self,
        peer: Address,
        outcome: PeerOutcome,
        failure: AttemptFailure,
    ) {
        self.add_peer_result(peer, outcome);
        self.fail_every_track(failure);
    }
}

impl<Number: Copy + Eq + Hash> FetchPool<Number> {
    /// Whether the pool is full: the most fetches running, or the most bytes
    /// being fetched
    pub fn is_full(&self, limits: &SpoolLimits) -> bool {
        let is_at_fetches = self.fetches.len() >= limits.max_fetches;
        is_at_fetches || self.bytes >= limits.max_fetch_bytes
    }

    /// Whether a fetch_req can start: the pool has room for its bytes, a
    /// recovery is under the most recoveries, and each of its peers is free
    pub fn can_start(
        &self,
        req: &FetchReq,
        scores: &PeerScores,
        now: WallTime,
        limits: &SpoolLimits,
    ) -> bool {
        let is_at_fetches = self.fetches.len() >= limits.max_fetches;
        if req.tracks.is_empty() || is_at_fetches {
            return false;
        }
        let bytes = self.bytes.saturating_add(req.bytes);
        if !self.bytes.is_zero() && bytes > limits.max_fetch_bytes {
            return false;
        }
        let is_recovery = req.method == Method::Recover;
        if is_recovery && self.recoveries >= limits.max_recoveries {
            return false;
        }
        for peer in &req.peers {
            let status = scores.get_peer_status(peer.peer, now, limits);
            if !status.is_free() && !status.can_probe() {
                return false;
            }
        }
        true
    }

    /// A fetch started under `number`: count what it holds
    pub fn add(&mut self, number: Number, req: FetchReq) {
        for track in &req.tracks {
            self.tracks.insert(track.missing_track.track);
        }
        self.bytes = self.bytes.saturating_add(req.bytes);
        if req.method == Method::Recover {
            self.recoveries += 1;
        }
        self.fetches.insert(number, req);
    }

    /// A fetch ended: let go of what it held, and give its fetch_req back
    pub fn remove(&mut self, number: Number) -> Option<FetchReq> {
        let req = self.fetches.remove(&number)?;
        for track in &req.tracks {
            self.tracks.remove(&track.missing_track.track);
        }
        self.bytes = self.bytes.saturating_sub(req.bytes);
        if req.method == Method::Recover {
            self.recoveries = self.recoveries.saturating_sub(1);
        }
        Some(req)
    }

    /// Whether a fetch for this track is running
    pub fn is_track_running(&self, track: Address) -> bool {
        self.tracks.contains(&track)
    }
}

impl<Number> Default for FetchPool<Number> {
    fn default() -> Self {
        Self {
            fetches: HashMap::new(),
            tracks: HashSet::new(),
            bytes: StorageUnits(0),
            recoveries: 0,
        }
    }
}

/// What one track counts toward a fetch_req for a method: its track_data
/// for a sync, and for the rest a share of its slice, when the slice's size
/// is known
pub fn get_track_bytes(
    method: Method,
    profile: ErasureCode,
    track: &CompressedTrack,
    slice: Option<StorageUnits>,
) -> StorageUnits {
    let Some(slice) = slice else {
        return track.size;
    };
    let k = StorageUnits(u64::from(profile.k()));
    match method {
        Method::Sync => track.size,
        Method::Transfer => slice,
        Method::Repair => get_repair_bytes(profile, slice),
        Method::Recover => slice.saturating_mul(k),
    }
}

/// A fetch gives up after the base time plus some for each MiB it asked for
pub fn get_time_limit(bytes: StorageUnits, limits: &SpoolLimits) -> Duration {
    let mib = u32::try_from(bytes.to_mb()).unwrap_or(u32::MAX);
    let per_mib = limits.fetch_time_per_mib.saturating_mul(mib);
    limits.fetch_timeout.saturating_add(per_mib)
}

/// A repair reads d helpers' share of a slice: d / (d - k + 1) slices
fn get_repair_bytes(profile: ErasureCode, slice: StorageUnits) -> StorageUnits {
    let Some(helpers) = profile.d() else {
        return slice;
    };
    let helpers = u64::from(helpers);
    let share = helpers.saturating_sub(u64::from(profile.k())) + 1;
    StorageUnits(slice.as_u64().saturating_mul(helpers) / share)
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;

    use super::*;

    fn coded_track() -> CompressedTrack {
        CompressedTrack {
            tape: Address::default(),
            tape_number: Default::default(),
            key: Default::default(),
            track_number: Default::default(),
            kind: 1,
            state: 1,
            flags: 0,
            size: StorageUnits::mb(1),
            group: VirtualGroup(0),
            value_hash: Default::default(),
        }
    }

    // a repair counts 1.6 slices and a recovery 7, on tapenet
    #[test]
    fn method_sizes() {
        let track = coded_track();
        let slice = Some(StorageUnits(1_000));
        let profile = TAPENET_PROFILE;

        let repair = get_track_bytes(Method::Repair, profile, &track, slice);
        let recovery = get_track_bytes(Method::Recover, profile, &track, slice);
        let sync = get_track_bytes(Method::Sync, profile, &track, slice);

        assert_eq!(repair, StorageUnits(1_600));
        assert_eq!(recovery, StorageUnits(7_000));
        assert_eq!(sync, track.size);
    }

    // a fetch gets 10 s, plus 8 s per MiB it asked for
    #[test]
    fn limit_grows() {
        let limits = SpoolLimits::default();

        let empty = get_time_limit(StorageUnits(0), &limits);
        let full = get_time_limit(StorageUnits::mb(8), &limits);

        assert_eq!(empty, Duration::from_secs(10));
        assert_eq!(full, Duration::from_secs(74));
    }
}
