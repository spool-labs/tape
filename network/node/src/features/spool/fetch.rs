//! What a fetch holds, what it counts against the room, and what it reports
//!
//! A fetch is one request, or a set of requests sent at once, for tracks
//! that all go by the same method. It runs on its own, checks what comes
//! back, writes it, and reports how each peer and each track went.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};

use tape_core::encoding::ErasureCode;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::api::ApiError;
use tape_slicer::coded_slice_len;
use tape_store::types::{AttemptResult, Entry, EntryKey, Method};
use tokio::task::{Id, JoinSet};
use tokio::time::timeout;

use crate::config::recovery::RecoveryConfig;
use crate::features::spool::attempt::Helper;
use crate::features::spool::peers::{PeerOutcome, PeerRules, PeerScores};

/// One track in a fetch, with what the turn read for it
#[derive(Clone, Debug)]
pub struct FetchTrack {
    pub key: EntryKey,
    pub entry: Entry,
    pub track: CompressedTrack,
    /// A coded track's encoding, when the store holds it
    pub blob: Option<BlobEncoding>,
    /// For a transfer, the epoch of the holder asked
    pub holder_epoch: EpochNumber,
}

/// Tracks that go by one method to the same peers
#[derive(Clone, Debug)]
pub struct Fetch {
    pub method: Method,
    /// Our live spool, as the fold of `live_groups` places it
    pub spool: SpoolIndex,
    pub live_groups: u64,
    pub peers: Vec<Helper>,
    pub tracks: Vec<FetchTrack>,
    pub bytes: StorageUnits,
}

/// How one peer did
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerResult {
    pub peer: Address,
    pub outcome: PeerOutcome,
}

/// How one track went
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrackOutcome {
    Got,
    Failed(AttemptResult),
}

/// How one track went, by its entry
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrackResult {
    pub key: EntryKey,
    pub outcome: TrackOutcome,
}

/// What a finished fetch reports; a track with no result was left out,
/// and its entry stays as it was
#[derive(Debug)]
pub struct FetchReport {
    pub fetch: Fetch,
    pub peers: Vec<PeerResult>,
    pub tracks: Vec<TrackResult>,
}

/// What a failed call says
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallFailure {
    /// The peer answered that it holds none of it
    NotHeld,
    /// The peer was down, too slow, or refused
    Peer(PeerOutcome),
}

/// A call's answer, and how long it took
#[derive(Debug)]
pub struct Answer<Response> {
    pub response: Result<Response, CallFailure>,
    pub elapsed: Duration,
}

/// Bytes that passed their check, for one track
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fetched {
    pub key: EntryKey,
    pub bytes: Vec<u8>,
}

/// What one peer's answer held, track by track
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Checked {
    pub good: Vec<Fetched>,
    pub wrong: Vec<EntryKey>,
    pub not_held: Vec<EntryKey>,
    pub received: StorageUnits,
}

/// What a fetch wrote to the store
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Written {
    pub keys: Vec<EntryKey>,
    pub bytes: StorageUnits,
}

/// The fetches running now, and the tracks and bytes in them
#[derive(Default)]
pub struct Running {
    pub tasks: JoinSet<FetchReport>,
    fetches: HashMap<Id, Held>,
    tracks: HashSet<Address>,
    bytes: StorageUnits,
    recoveries: usize,
}

/// What one running fetch holds, to let go of when it ends
#[derive(Debug)]
pub struct Held {
    pub method: Method,
    pub peers: Vec<Address>,
    pub tracks: Vec<Address>,
    pub bytes: StorageUnits,
}

impl Written {
    /// One track's bytes went to the store
    pub fn add(&mut self, key: EntryKey, bytes: StorageUnits) {
        self.keys.push(key);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

impl Fetch {
    /// A fetch with no tracks yet
    pub fn new(
        method: Method,
        spool: SpoolIndex,
        live_groups: u64,
        peers: Vec<Helper>,
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

    /// Whether the fetch holds all it may: one track for a repair, since a
    /// repair request carries one, else the most tracks or bytes
    pub fn is_full(&self, config: &RecoveryConfig) -> bool {
        if self.method == Method::Repair {
            return !self.tracks.is_empty();
        }
        let is_at_tracks = self.tracks.len() >= config.fetch_tracks;
        is_at_tracks || self.bytes >= config.fetch_bytes
    }

    /// Whether another track of `bytes` would take the fetch past its bytes
    pub fn would_overflow(
        &self,
        bytes: StorageUnits,
        config: &RecoveryConfig,
    ) -> bool {
        let total = self.bytes.saturating_add(bytes);
        !self.tracks.is_empty() && total > config.fetch_bytes
    }

    /// Add a track and what it counts
    pub fn push(&mut self, track: FetchTrack, bytes: StorageUnits) {
        self.tracks.push(track);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// The tracks' addresses, in the fetch's order
    pub fn addresses(&self) -> Vec<Address> {
        let mut addresses = Vec::with_capacity(self.tracks.len());
        for track in &self.tracks {
            addresses.push(track.key.track);
        }
        addresses
    }

    /// The fetch's track with this address
    pub fn track(&self, address: Address) -> Option<&FetchTrack> {
        let is_asked = |track: &&FetchTrack| track.key.track == address;
        self.tracks.iter().find(is_asked)
    }
}

impl FetchReport {
    /// A report with nothing in it yet
    pub fn new(fetch: Fetch) -> Self {
        Self { fetch, peers: Vec::new(), tracks: Vec::new() }
    }

    /// Record how a peer did
    pub fn peer(&mut self, peer: Address, outcome: PeerOutcome) {
        self.peers.push(PeerResult { peer, outcome });
    }

    /// Record how a track went
    pub fn track(&mut self, key: EntryKey, outcome: TrackOutcome) {
        self.tracks.push(TrackResult { key, outcome });
    }

    /// Every track of the fetch went the same way
    pub fn every_track(&mut self, outcome: TrackOutcome) {
        for index in 0..self.fetch.tracks.len() {
            let key = self.fetch.tracks[index].key;
            self.track(key, outcome);
        }
    }

    /// A call to `peer` failed, so every track went with it
    pub fn failed_call(
        &mut self,
        peer: Address,
        failure: CallFailure,
        elapsed: Duration,
    ) {
        match failure {
            CallFailure::NotHeld => {
                let bytes = StorageUnits(0);
                self.peer(peer, PeerOutcome::Worked { bytes, elapsed });
                let not_held = AttemptResult::NotHeld;
                self.every_track(TrackOutcome::Failed(not_held));
            }
            CallFailure::Peer(outcome) => {
                self.peer(peer, outcome);
                self.every_track(failed_track(outcome));
            }
        }
    }

    /// One peer answered: the tracks written are got, the rest go as checked
    pub fn answered(
        &mut self,
        peer: Address,
        checked: &Checked,
        written: &[EntryKey],
        elapsed: Duration,
    ) {
        let bytes = checked.received;
        let mut outcome = PeerOutcome::Worked { bytes, elapsed };
        if !checked.wrong.is_empty() {
            outcome = PeerOutcome::WrongData;
        }
        self.peer(peer, outcome);
        for key in written {
            self.track(*key, TrackOutcome::Got);
        }
        let wrong = TrackOutcome::Failed(AttemptResult::WrongData);
        for key in &checked.wrong {
            self.track(*key, wrong);
        }
        let not_held = TrackOutcome::Failed(AttemptResult::NotHeld);
        for key in &checked.not_held {
            self.track(*key, not_held);
        }
    }
}

impl Running {
    /// Room for another fetch: fewer than the most running, and fewer than
    /// the most bytes being fetched
    pub fn has_room(&self, config: &RecoveryConfig) -> bool {
        let has_fetches = self.fetches.len() < config.max_fetches;
        has_fetches && self.bytes < config.max_fetch_bytes
    }

    /// Whether a fetch for this track is running
    pub fn is_running(&self, track: Address) -> bool {
        self.tracks.contains(&track)
    }

    /// Room for this fetch: the room above counting its bytes, a recovery
    /// under the most recoveries, and every one of its peers free
    pub fn can_start(
        &self,
        fetch: &Fetch,
        config: &RecoveryConfig,
        peers: &PeerScores,
        now: Instant,
        rules: &PeerRules,
    ) -> bool {
        let is_full = self.fetches.len() >= config.max_fetches;
        if fetch.tracks.is_empty() || is_full {
            return false;
        }
        let bytes = self.bytes.saturating_add(fetch.bytes);
        if !self.bytes.is_zero() && bytes > config.max_fetch_bytes {
            return false;
        }
        let is_recovery = fetch.method == Method::Recovery;
        if is_recovery && self.recoveries >= config.max_recoveries {
            return false;
        }
        for helper in &fetch.peers {
            let status = peers.status(helper.peer, now, rules);
            if !status.is_free() && !status.can_probe() {
                return false;
            }
        }
        true
    }

    /// A fetch started: count what it holds
    pub fn hold(&mut self, id: Id, held: Held) {
        for track in &held.tracks {
            self.tracks.insert(*track);
        }
        self.bytes = self.bytes.saturating_add(held.bytes);
        if held.method == Method::Recovery {
            self.recoveries += 1;
        }
        self.fetches.insert(id, held);
    }

    /// A fetch ended: let go of what it held
    pub fn release(&mut self, id: Id) -> Option<Held> {
        let held = self.fetches.remove(&id)?;
        for track in &held.tracks {
            self.tracks.remove(track);
        }
        self.bytes = self.bytes.saturating_sub(held.bytes);
        if held.method == Method::Recovery {
            self.recoveries = self.recoveries.saturating_sub(1);
        }
        Some(held)
    }
}

impl Held {
    /// What a fetch holds while it runs
    pub fn of(fetch: &Fetch) -> Self {
        let mut peers = Vec::with_capacity(fetch.peers.len());
        for helper in &fetch.peers {
            peers.push(helper.peer);
        }
        Self {
            method: fetch.method,
            peers,
            tracks: fetch.addresses(),
            bytes: fetch.bytes,
        }
    }
}

/// How a call that failed reads: an answer of not holding the data is the
/// peer working; anything else is the peer down, too slow, or refusing
pub fn call_failure(error: &ApiError) -> CallFailure {
    let refused = PeerOutcome::Refused { wait: None };
    match error {
        ApiError::NotFound | ApiError::NotResponsible => CallFailure::NotHeld,
        ApiError::ConnectionFailed(_) | ApiError::NodeUnresolved(_) => {
            CallFailure::Peer(PeerOutcome::Down)
        }
        ApiError::Timeout => CallFailure::Peer(PeerOutcome::TooSlow),
        ApiError::RateLimited { retry_after } => {
            let wait = *retry_after;
            CallFailure::Peer(PeerOutcome::Refused { wait })
        }
        ApiError::Serialization(_)
        | ApiError::ServerError { .. }
        | ApiError::BlacklistedObject
        | ApiError::NotInCommittee
        | ApiError::StaleTrackProof
        | ApiError::Other(_) => CallFailure::Peer(refused),
    }
}

/// What a peer failure counts as for the tracks it held up
pub fn failed_track(outcome: PeerOutcome) -> TrackOutcome {
    let result = match outcome {
        PeerOutcome::Down => AttemptResult::Down,
        PeerOutcome::TooSlow => AttemptResult::TooSlow,
        PeerOutcome::WrongData => AttemptResult::WrongData,
        PeerOutcome::Refused { .. } | PeerOutcome::Worked { .. } => {
            AttemptResult::Refused
        }
    };
    TrackOutcome::Failed(result)
}

/// Send one call, giving up after `limit`
pub async fn send<Response, Call>(
    limit: Duration,
    call: Call,
) -> Answer<Response>
where
    Call: Future<Output = Result<Response, ApiError>>,
{
    let started = Instant::now();
    let sent = timeout(limit, call).await;
    let elapsed = started.elapsed();
    let response = match sent {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) => Err(call_failure(&error)),
        Err(_) => Err(CallFailure::Peer(PeerOutcome::TooSlow)),
    };
    Answer { response, elapsed }
}

/// A fetch gives up after the base time plus some for each MiB it asked for
pub fn time_limit(bytes: StorageUnits, config: &RecoveryConfig) -> Duration {
    let per_mib = config.fetch_secs_per_mib.saturating_mul(bytes.to_mb());
    Duration::from_secs(config.fetch_timeout_secs.saturating_add(per_mib))
}

/// What one track counts against the room for a method
pub fn track_bytes(
    method: Method,
    profile: ErasureCode,
    track: &CompressedTrack,
    blob: Option<&BlobEncoding>,
) -> StorageUnits {
    let Some(blob) = blob else {
        return track.size;
    };
    let slice = slice_bytes(profile, blob);
    let k = StorageUnits(u64::from(profile.k()));
    match method {
        Method::Sync | Method::Upload => track.size,
        Method::Transfer => slice,
        Method::Repair => repair_bytes(profile, slice),
        Method::Recovery => slice.saturating_mul(k),
    }
}

/// The size of one slice of a coded track
pub fn slice_bytes(profile: ErasureCode, blob: &BlobEncoding) -> StorageUnits {
    let len = coded_slice_len(
        profile,
        blob.size.as_usize(),
        blob.stripe_size.as_usize(),
        blob.stripe_count.as_usize(),
    );
    StorageUnits(len as u64)
}

/// A repair reads d helpers' share of a slice: d / (d - k + 1) slices
fn repair_bytes(profile: ErasureCode, slice: StorageUnits) -> StorageUnits {
    let ErasureCode::Clay(params) = profile else {
        return slice;
    };
    let helpers = u64::from(params.d());
    let share = helpers.saturating_sub(u64::from(params.k())) + 1;
    StorageUnits(slice.as_u64().saturating_mul(helpers) / share)
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::types::StripeCount;

    use super::*;

    fn coded_track() -> (CompressedTrack, BlobEncoding) {
        let blob = BlobEncoding {
            size: StorageUnits::mb(1),
            commitment: Default::default(),
            stripe_size: StorageUnits::mb(1),
            stripe_count: StripeCount(1),
            leaves: Default::default(),
        };
        let track = CompressedTrack {
            tape: Address::default(),
            tape_number: Default::default(),
            key: Default::default(),
            track_number: Default::default(),
            kind: 1,
            state: 1,
            flags: 0,
            size: blob.size,
            group: VirtualGroup(0),
            value_hash: Default::default(),
        };
        (track, blob)
    }

    // a repair counts 1.6 slices and a recovery 7, on tapenet
    #[test]
    fn method_sizes() {
        let (track, blob) = coded_track();
        let slice = slice_bytes(TAPENET_PROFILE, &blob).as_u64();
        let profile = TAPENET_PROFILE;

        let blob = Some(&blob);
        let repair = track_bytes(Method::Repair, profile, &track, blob);
        let recovery = track_bytes(Method::Recovery, profile, &track, blob);

        assert_eq!(repair.as_u64(), slice * 16 / 10);
        assert_eq!(recovery.as_u64(), slice * 7);
    }

    // a fetch gets 5 s, plus 1 s per MiB it asked for
    #[test]
    fn limit_grows() {
        let config = RecoveryConfig::default();

        let empty = time_limit(StorageUnits(0), &config);
        let full = time_limit(StorageUnits::mb(8), &config);

        assert_eq!(empty, Duration::from_secs(5));
        assert_eq!(full, Duration::from_secs(13));
    }

    // a peer that holds none of it worked; a refusal keeps the wait it
    // asked for
    #[test]
    fn failures_read() {
        let wait = Some(Duration::from_secs(9));
        let limited = ApiError::RateLimited { retry_after: wait };
        let refused = CallFailure::Peer(PeerOutcome::Refused { wait });
        let slow = CallFailure::Peer(PeerOutcome::TooSlow);
        let lacking = ApiError::NotResponsible;

        assert_eq!(call_failure(&lacking), CallFailure::NotHeld);
        assert_eq!(call_failure(&limited), refused);
        assert_eq!(call_failure(&ApiError::Timeout), slow);
    }
}
