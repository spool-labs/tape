use std::cmp::Reverse;

use tape_core::encoding::ErasureCode;
use tape_crypto::address::Address;

use std::time::Duration;

use crate::history::{Holder, PositionPeer};
use crate::limits::SpoolLimits;
use crate::peer::{PeerScores, PeerStatus};
use crate::time::WallTime;
use crate::track::{MissingTrack, TrackOrigin};

/// The ways to get what a track is missing, best first
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Method {
    Sync,
    Transfer,
    Repair,
    Recover,
}

/// One try with one method that didn't get the track what it lacks: when,
/// how, and what went wrong
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Attempt {
    /// When the attempt was made
    pub at: WallTime,
    /// The method it tried
    pub method: Method,
    /// What went wrong
    pub failure: AttemptFailure,
}

/// Why an attempt didn't get the track what it lacks
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AttemptFailure {
    /// The peer couldn't be reached
    Down,
    /// The peer ran past the fetch's time limit
    TooSlow,
    /// The peer refused
    Refused,
    /// The peer doesn't have it
    NotHeld,
    /// The peer sent bytes that failed their check
    WrongData,
}

/// What a turn decides for one missing track
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NextAttempt {
    /// Get the track_data from this sync_peer
    Sync { peer: Address },
    /// Get our slice whole from this holder
    Transfer { holder: Holder },
    /// Rebuild our slice from these helpers
    Repair { helpers: Vec<PositionPeer> },
    /// Rebuild the track from these peers' whole slices
    Recover { peers: Vec<PositionPeer> },
    /// The missing track can't go right now; a later turn looks again
    NotNow,
    /// The peers it needs serve our other fetches; the turn stops here
    Busy,
}

impl Method {
    /// The method's byte in a missing track's layout
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Sync => 1,
            Self::Transfer => 2,
            Self::Repair => 3,
            Self::Recover => 4,
        }
    }

    /// The method a layout byte holds, nothing for zero or a byte past them
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Sync),
            2 => Some(Self::Transfer),
            3 => Some(Self::Repair),
            4 => Some(Self::Recover),
            _ => None,
        }
    }
}

impl AttemptFailure {
    /// A failure of the track's own: the peer doesn't have it, or sent wrong
    /// data. The rest are failures of the peer.
    pub fn is_track_failure(self) -> bool {
        matches!(self, Self::NotHeld | Self::WrongData)
    }

    /// The failure's byte in a missing track's layout
    pub fn to_byte(self) -> u8 {
        match self {
            Self::Down => 1,
            Self::TooSlow => 2,
            Self::Refused => 3,
            Self::NotHeld => 4,
            Self::WrongData => 5,
        }
    }

    /// The failure a layout byte holds, nothing for zero or a byte past them
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Down),
            2 => Some(Self::TooSlow),
            3 => Some(Self::Refused),
            4 => Some(Self::NotHeld),
            5 => Some(Self::WrongData),
            _ => None,
        }
    }
}

/// Transfer, then repair, then recovery, always from the top, so a holder
/// that comes back takes over again
pub fn choose_attempt(
    missing: &MissingTrack,
    holders: &[Holder],
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    profile: ErasureCode,
    now: WallTime,
    limits: &SpoolLimits,
) -> NextAttempt {
    if let Some(attempt) = can_transfer(missing, holders, scores, now, limits) {
        return attempt;
    }
    let repair = can_repair(missing, group_peers, scores, profile, now, limits);
    if let Some(attempt) = repair {
        return attempt;
    }
    can_recover(missing, group_peers, scores, profile, now, limits)
}

/// Ask the holders newest first, only those older than the last that said
/// no. One down or slow keeps the track waiting, and only gone ones are
/// passed over. Nothing means repair may start.
pub fn can_transfer(
    missing: &MissingTrack,
    holders: &[Holder],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> Option<NextAttempt> {
    let candidates = get_candidates(missing, holders);
    let newest = get_newest(&candidates, scores, now, limits);
    if let Some((holder, status)) = newest {
        if status.is_free() {
            return Some(NextAttempt::Transfer { holder });
        }
    }
    if let Some(holder) = get_probe(&candidates, scores, now, limits) {
        return Some(NextAttempt::Transfer { holder });
    }
    let (_, status) = newest?;
    if status.is_resting() {
        return Some(NextAttempt::NotNow);
    }
    Some(NextAttempt::Busy)
}

/// Rebuild our slice from d helpers at other positions, while repairs
/// haven't failed for too long; nothing means recovery may start. Each
/// failure of the track moves it on to the next helpers in speed order.
pub fn can_repair(
    missing: &MissingTrack,
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    profile: ErasureCode,
    now: WallTime,
    limits: &SpoolLimits,
) -> Option<NextAttempt> {
    let helpers = usize::from(profile.d()?);
    let has_failed = has_repairs_failed(missing, now, limits);
    if has_failed || was_wrong_repair(missing) {
        return None;
    }
    let usable = get_positions(group_peers, scores, now, limits, is_usable);
    if usable.len() < helpers {
        return None;
    }
    let start = usize::from(missing.failures);
    let mut free = get_free_helpers(group_peers, scores, now, limits, start);
    if free.len() >= helpers {
        free.truncate(helpers);
        return Some(NextAttempt::Repair { helpers: free });
    }
    let awake = get_positions(group_peers, scores, now, limits, is_awake);
    if awake.len() >= helpers {
        return Some(NextAttempt::Busy);
    }
    Some(NextAttempt::NotNow)
}

/// Get k whole slices from k positions. Each failure of the track moves it
/// on to the next peers in speed order.
pub fn can_recover(
    missing: &MissingTrack,
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    profile: ErasureCode,
    now: WallTime,
    limits: &SpoolLimits,
) -> NextAttempt {
    let k = usize::from(profile.k());
    let usable = get_positions(group_peers, scores, now, limits, is_usable);
    if usable.len() < k {
        return NextAttempt::NotNow;
    }
    let start = usize::from(missing.failures);
    let mut free = get_free_helpers(group_peers, scores, now, limits, start);
    if free.len() >= k {
        free.truncate(k);
        return NextAttempt::Recover { peers: free };
    }
    let awake = get_positions(group_peers, scores, now, limits, is_awake);
    if awake.len() >= k {
        return NextAttempt::Busy;
    }
    NextAttempt::NotNow
}

/// Whether the turn can try a missing track now. A never-tried handover
/// goes at once, and a never-tried upload waits `upload_wait` for the
/// uploader to deliver. After a failure of the track's own a sync, repair
/// or recovery waits 1 s doubled per failure; after a failure of the peer,
/// the peer's rest decides.
pub fn can_try_now(
    missing: &MissingTrack,
    now: WallTime,
    upload_wait: Duration,
    limits: &SpoolLimits,
) -> bool {
    let Some(attempt) = missing.last_attempt else {
        return can_try_first(missing, now, upload_wait);
    };
    let waited = now.get_time_since(attempt.at);
    let wait = limits.get_track_wait(missing.failures);
    let is_track_failure = attempt.failure.is_track_failure();
    match attempt.method {
        Method::Transfer => true,
        Method::Sync | Method::Repair | Method::Recover => {
            !is_track_failure || waited >= wait
        }
    }
}

/// A free sync_peer, a probe when none is free, busy while one serves our
/// other fetches, and not_now otherwise. The free peers go in speed order,
/// and each failure of the track moves it on to the next one, so a track
/// that one peer didn't have goes to another.
pub fn pick_sync_peer(
    missing: &MissingTrack,
    peers: &[Address],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> NextAttempt {
    let mut statuses = Vec::with_capacity(peers.len());
    for peer in peers {
        statuses.push(scores.get_peer_status(*peer, now, limits));
    }
    let free = get_free_peers(peers, scores, now, limits);
    if !free.is_empty() {
        let start = usize::from(missing.failures);
        return NextAttempt::Sync { peer: free[start % free.len()] };
    }
    for (peer, status) in peers.iter().zip(&statuses) {
        if status.can_probe() {
            return NextAttempt::Sync { peer: *peer };
        }
    }
    if statuses.contains(&PeerStatus::Busy) {
        return NextAttempt::Busy;
    }
    NextAttempt::NotNow
}

/// Holders older than the last one that said no, newest first
fn get_candidates(missing: &MissingTrack, holders: &[Holder]) -> Vec<Holder> {
    let tried = missing.owner_epoch_tried;
    let mut candidates = Vec::new();
    for holder in holders {
        if tried.is_zero() || holder.epoch < tried {
            candidates.push(*holder);
        }
    }
    candidates
}

/// The newest candidate that isn't gone, with its status
fn get_newest(
    candidates: &[Holder],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> Option<(Holder, PeerStatus)> {
    for holder in candidates {
        let status = scores.get_peer_status(holder.node, now, limits);
        if !status.is_gone() {
            return Some((*holder, status));
        }
    }
    None
}

/// The first gone candidate whose rest has ended
fn get_probe(
    candidates: &[Holder],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> Option<Holder> {
    for holder in candidates {
        let status = scores.get_peer_status(holder.node, now, limits);
        if status.can_probe() {
            return Some(*holder);
        }
    }
    None
}

fn has_repairs_failed(
    missing: &MissingTrack,
    now: WallTime,
    limits: &SpoolLimits,
) -> bool {
    let Some(since) = missing.repair_failing_since else {
        return false;
    };
    now.get_time_since(since) >= limits.repair_failing
}

/// A repair whose slice failed its check had a helper that sent wrong data.
/// Nothing tells which one, and the next repair would ask it again, so the
/// track goes on to recovery.
fn was_wrong_repair(missing: &MissingTrack) -> bool {
    let Some(attempt) = missing.last_attempt else {
        return false;
    };
    let is_wrong = attempt.failure == AttemptFailure::WrongData;
    attempt.method == Method::Repair && is_wrong
}

/// The positions with a peer whose status passes `wanted`, once each
fn get_positions(
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
    wanted: fn(PeerStatus) -> bool,
) -> Vec<usize> {
    let mut positions = Vec::new();
    for helper in group_peers {
        let status = scores.get_peer_status(helper.peer, now, limits);
        if wanted(status) && !positions.contains(&helper.position) {
            positions.push(helper.position);
        }
    }
    positions
}

fn is_usable(status: PeerStatus) -> bool {
    !status.is_gone()
}

fn is_awake(status: PeerStatus) -> bool {
    matches!(status, PeerStatus::Free { .. } | PeerStatus::Busy)
}

/// One free peer for each position that has one. The walk goes through the
/// free peers in speed order from the `start`-th on, wrapping round, and
/// takes the first peer it meets at each position.
fn get_free_helpers(
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
    start: usize,
) -> Vec<PositionPeer> {
    let free = get_free_by_speed(group_peers, scores, now, limits);
    let mut helpers: Vec<PositionPeer> = Vec::new();
    for offset in 0..free.len() {
        let helper = free[(start + offset) % free.len()];
        if !has_position(&helpers, helper.position) {
            helpers.push(helper);
        }
    }
    helpers
}

/// Every free peer of the group, fastest first; one never measured counts
/// as fastest
fn get_free_by_speed(
    group_peers: &[PositionPeer],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> Vec<PositionPeer> {
    let mut free = Vec::new();
    for helper in group_peers {
        let status = scores.get_peer_status(helper.peer, now, limits);
        if status.is_free() {
            free.push(*helper);
        }
    }
    let get_rank = |helper: &PositionPeer| {
        let status = scores.get_peer_status(helper.peer, now, limits);
        Reverse(status.get_speed_rank())
    };
    free.sort_by_key(get_rank);
    free
}

fn has_position(helpers: &[PositionPeer], position: usize) -> bool {
    for helper in helpers {
        if helper.position == position {
            return true;
        }
    }
    false
}

/// A never-tried missing track: a handover goes at once, and an upload once
/// the uploader's time to deliver has passed since it was added
fn can_try_first(
    missing: &MissingTrack,
    now: WallTime,
    upload_wait: Duration,
) -> bool {
    match missing.origin {
        TrackOrigin::Handover => true,
        TrackOrigin::Upload => {
            now.get_time_since(missing.added_at) >= upload_wait
        }
    }
}

/// The free peers, fastest first; one never measured counts as fastest
fn get_free_peers(
    peers: &[Address],
    scores: &PeerScores,
    now: WallTime,
    limits: &SpoolLimits,
) -> Vec<Address> {
    let mut free = Vec::new();
    for peer in peers {
        if scores.get_peer_status(*peer, now, limits).is_free() {
            free.push(*peer);
        }
    }
    let get_rank = |peer: &Address| {
        let status = scores.get_peer_status(*peer, now, limits);
        Reverse(status.get_speed_rank())
    };
    free.sort_by_key(get_rank);
    free
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};
    use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};

    use super::*;
    use crate::peer::PeerOutcome;

    /// Three of four slices rebuild a track on this profile
    const SMALL: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    /// The wall time every test decides at
    const NOW: u64 = 10_000;

    /// The uploader's time to deliver
    const UPLOAD_WAIT: Duration = Duration::from_secs(2);

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn secs(count: u64) -> WallTime {
        WallTime(count * 1_000)
    }

    fn limits() -> SpoolLimits {
        SpoolLimits::default()
    }

    fn fresh() -> MissingTrack {
        MissingTrack::new_handover(SpoolIndex(0), address(0), secs(0))
    }

    fn tried(
        method: Method,
        failure: AttemptFailure,
        at: WallTime,
    ) -> MissingTrack {
        let attempt = Attempt { at, method, failure };
        MissingTrack { last_attempt: Some(attempt), ..fresh() }
    }

    fn holder(byte: u8, epoch: u64) -> Holder {
        Holder { node: address(byte), epoch: EpochNumber(epoch) }
    }

    /// A peer as fast as `speed` bytes a second
    fn measure(scores: &mut PeerScores, peer: Address, speed: u64) {
        let bytes = StorageUnits(speed);
        let elapsed = Duration::from_secs(1);
        let worked = PeerOutcome::Worked { bytes, elapsed };
        scores.record_peer_outcome(peer, worked, secs(NOW), &limits());
    }

    /// A peer resting after a failure now
    fn rest(scores: &mut PeerScores, peer: Address) {
        let down = PeerOutcome::Down;
        scores.record_peer_outcome(peer, down, secs(NOW), &limits());
    }

    /// A peer serving as many of our fetches as it may
    fn occupy(scores: &mut PeerScores, peer: Address) {
        for _ in 0..limits().max_peer_fetches {
            scores.start_fetch(peer);
        }
    }

    /// A peer failing for 10 min, whose rest has ended
    fn lose(scores: &mut PeerScores, peer: Address) {
        let down = PeerOutcome::Down;
        let gone = secs(NOW - 600);
        scores.record_peer_outcome(peer, down, gone, &limits());
    }

    /// One peer at each of `count` positions, peer byte = position + 1
    fn group(count: usize) -> Vec<PositionPeer> {
        let mut peers = Vec::new();
        for position in 0..count {
            let peer = address(position as u8 + 1);
            peers.push(PositionPeer { position, peer });
        }
        peers
    }

    fn positions(helpers: &[PositionPeer]) -> Vec<usize> {
        let mut positions = Vec::new();
        for helper in helpers {
            positions.push(helper.position);
        }
        positions
    }

    fn can_repair_now(
        missing: &MissingTrack,
        group_peers: &[PositionPeer],
        scores: &PeerScores,
        at: u64,
    ) -> Option<NextAttempt> {
        let profile = TAPENET_PROFILE;
        can_repair(missing, group_peers, scores, profile, secs(at), &limits())
    }

    /// A repair's helpers, none for any other attempt
    fn get_helpers(attempt: Option<NextAttempt>) -> Vec<PositionPeer> {
        if let Some(NextAttempt::Repair { helpers }) = attempt {
            return helpers;
        }
        Vec::new()
    }

    // a never-tried handover goes at once, a never-tried upload waits for
    // the uploader from when it was added, and a peer's failure leaves the
    // rest to the peer
    #[test]
    fn try_now_rules() {
        let spool = SpoolIndex(0);
        let handover = MissingTrack::new_handover(spool, address(0), secs(10));
        let uploaded = MissingTrack::new_upload(spool, address(0), secs(10));
        let down = tried(Method::Sync, AttemptFailure::Down, secs(10));

        let try_now = |missing, at| {
            can_try_now(missing, secs(at), UPLOAD_WAIT, &limits())
        };
        assert!(try_now(&handover, 10));
        assert!(!try_now(&uploaded, 11));
        assert!(try_now(&uploaded, 12));
        assert!(try_now(&down, 10));
    }

    // a failure of the track's own waits 1 s doubled per failure, up to 1 min
    #[test]
    fn track_wait_doubles() {
        let not_held = AttemptFailure::NotHeld;
        let mut missing = tried(Method::Repair, not_held, secs(100));
        missing.failures = 3;

        let try_now = |missing: &MissingTrack, at| {
            can_try_now(missing, secs(at), UPLOAD_WAIT, &limits())
        };
        assert!(!try_now(&missing, 103));
        assert!(try_now(&missing, 104));
        missing.failures = 20;
        assert!(!try_now(&missing, 159));
        assert!(try_now(&missing, 160));
    }

    // holders go newest first, and a resting newest holder keeps the track
    // waiting
    #[test]
    fn newest_holder() {
        let holders = [holder(1, 9), holder(2, 8)];
        let mut scores = PeerScores::default();
        let now = secs(NOW);

        let free = can_transfer(&fresh(), &holders, &scores, now, &limits());
        rest(&mut scores, address(1));
        let resting = can_transfer(&fresh(), &holders, &scores, now, &limits());

        let transfer = NextAttempt::Transfer { holder: holders[0] };
        assert_eq!(free, Some(transfer));
        assert_eq!(resting, Some(NextAttempt::NotNow));
    }

    // a holder that said no passes the track to older ones, and none left
    // means repair may start
    #[test]
    fn holders_said_no() {
        let holders = [holder(1, 9), holder(2, 8)];
        let scores = PeerScores::default();
        let now = secs(NOW);
        let mut missing = fresh();

        missing.owner_epoch_tried = EpochNumber(9);
        let older = can_transfer(&missing, &holders, &scores, now, &limits());
        missing.owner_epoch_tried = EpochNumber(8);
        let none = can_transfer(&missing, &holders, &scores, now, &limits());

        assert_eq!(older, Some(NextAttempt::Transfer { holder: holders[1] }));
        assert_eq!(none, None);
    }

    // a gone holder is passed over, or probed once its rest ends
    #[test]
    fn gone_holder() {
        let holders = [holder(1, 9), holder(2, 8)];
        let mut scores = PeerScores::default();
        let now = secs(NOW);
        lose(&mut scores, address(1));
        occupy(&mut scores, address(1));

        let only = &holders[..1];
        let passed = can_transfer(&fresh(), &holders, &scores, now, &limits());
        let none = can_transfer(&fresh(), only, &scores, now, &limits());
        for _ in 0..limits().max_peer_fetches {
            scores.finish_fetch(address(1));
        }
        let probed = can_transfer(&fresh(), only, &scores, now, &limits());

        let next = NextAttempt::Transfer { holder: holders[1] };
        assert_eq!(passed, Some(next));
        assert_eq!(none, None);
        let probe = NextAttempt::Transfer { holder: holders[0] };
        assert_eq!(probed, Some(probe));
    }

    // healthy holders serving our other fetches keep the track waiting for
    // them
    #[test]
    fn busy_holders() {
        let holders = [holder(1, 9)];
        let mut scores = PeerScores::default();
        occupy(&mut scores, address(1));
        let group = group(19);

        let attempt = choose_attempt(
            &fresh(),
            &holders,
            &group,
            &scores,
            TAPENET_PROFILE,
            secs(NOW),
            &limits(),
        );

        assert_eq!(attempt, NextAttempt::Busy);
    }

    // a short outage of the only holder keeps the track at transfer, and
    // nothing falls through to repair
    #[test]
    fn short_outage() {
        let missing = tried(Method::Transfer, AttemptFailure::Down, secs(NOW));
        let holders = [holder(1, 9)];
        let mut scores = PeerScores::default();
        rest(&mut scores, address(1));

        let attempt = choose_attempt(
            &missing,
            &holders,
            &group(19),
            &scores,
            TAPENET_PROFILE,
            secs(NOW + 1),
            &limits(),
        );

        assert_eq!(attempt, NextAttempt::NotNow);
    }

    // repairs failing for 5 min, or a repair whose slice was wrong, give way
    // to recovery
    #[test]
    fn repair_gives_way() {
        let group = group(19);
        let scores = PeerScores::default();
        let mut failing = fresh();
        failing.repair_failing_since = Some(secs(0));
        let wrong = tried(Method::Repair, AttemptFailure::WrongData, secs(0));

        let early = can_repair_now(&failing, &group, &scores, 299);
        let late = can_repair_now(&failing, &group, &scores, 300);
        let after_wrong = can_repair_now(&wrong, &group, &scores, 1);

        assert!(matches!(early, Some(NextAttempt::Repair { .. })));
        assert_eq!(late, None);
        assert_eq!(after_wrong, None);
    }

    // repair takes d helpers, waits while they serve or rest, and gives way
    // when fewer than d positions are usable or the code has no repair
    #[test]
    fn repair_helpers() {
        let group = group(19);
        let mut scores = PeerScores::default();

        let free = can_repair_now(&fresh(), &group, &scores, NOW);
        for helper in &group[..4] {
            occupy(&mut scores, helper.peer);
        }
        let busy = can_repair_now(&fresh(), &group, &scores, NOW);
        for helper in &group[4..8] {
            rest(&mut scores, helper.peer);
        }
        let resting = can_repair_now(&fresh(), &group, &scores, NOW);
        for helper in &group[..4] {
            lose(&mut scores, helper.peer);
        }
        let few = can_repair_now(&fresh(), &group, &scores, NOW);
        let limits = limits();
        let off_clay = can_repair(&fresh(), &group, &scores, SMALL, secs(NOW), &limits);

        let d = usize::from(TAPENET_PROFILE.d().expect("clay"));
        assert_eq!(get_helpers(free).len(), d);
        assert_eq!(busy, Some(NextAttempt::Busy));
        assert_eq!(resting, Some(NextAttempt::NotNow));
        assert_eq!(few, None);
        assert_eq!(off_clay, None);
    }

    // repair takes only the d fastest free positions, so a slow one sits out
    #[test]
    fn repair_skips_slow() {
        let group = group(17);
        let mut scores = PeerScores::default();
        measure(&mut scores, group[0].peer, 1);
        for helper in &group[1..] {
            measure(&mut scores, helper.peer, 500);
        }

        let choice = can_repair_now(&fresh(), &group, &scores, NOW);

        let expected: Vec<usize> = (1..17).collect();
        assert_eq!(positions(&get_helpers(choice)), expected);
    }

    // recovery takes the k fastest free positions, waits on busy ones, and
    // can't go with fewer than k usable
    #[test]
    fn recovery_peers() {
        let mut scores = PeerScores::default();
        let now = secs(NOW);
        let limits = limits();
        let fastest = group(4);
        measure(&mut scores, fastest[0].peer, 10);
        for helper in &fastest[1..] {
            measure(&mut scores, helper.peer, 900);
        }

        let missing = fresh();
        let recover = |peers: &[PositionPeer], scores: &PeerScores| {
            can_recover(&missing, peers, scores, SMALL, now, &limits)
        };

        let quick = recover(&fastest, &scores);
        occupy(&mut scores, fastest[3].peer);
        let busy = recover(&fastest[1..], &scores);
        lose(&mut scores, fastest[2].peer);
        lose(&mut scores, fastest[3].peer);
        let short = recover(&fastest, &scores);

        let expected = vec![fastest[1], fastest[2], fastest[3]];
        assert_eq!(quick, NextAttempt::Recover { peers: expected });
        assert_eq!(busy, NextAttempt::Busy);
        assert_eq!(short, NextAttempt::NotNow);
    }

    // pick_sync_peer gives the fastest free peer, never measured first, then
    // busy, then not_now
    #[test]
    fn pick_fastest() {
        let mut scores = PeerScores::default();
        let now = secs(NOW);
        let limits = limits();
        let (measured, unseen, busy, resting) =
            (address(1), address(2), address(3), address(4));
        measure(&mut scores, measured, 5);
        occupy(&mut scores, busy);
        rest(&mut scores, resting);
        let missing = fresh();
        let pick = |peers: &[Address]| {
            pick_sync_peer(&missing, peers, &scores, now, &limits)
        };

        let first = pick(&[measured, unseen]);
        let waiting = pick(&[busy, resting]);
        let rested = pick(&[resting]);

        assert_eq!(first, NextAttempt::Sync { peer: unseen });
        assert_eq!(waiting, NextAttempt::Busy);
        assert_eq!(rested, NextAttempt::NotNow);
    }

    // each failure of a track moves its sync on to the next free peer in
    // speed order, and the order wraps round
    #[test]
    fn sync_moves_on() {
        let mut scores = PeerScores::default();
        let now = secs(NOW);
        let limits = limits();
        let (fast, slow) = (address(1), address(2));
        measure(&mut scores, fast, 900);
        measure(&mut scores, slow, 10);
        let mut missing = tried(Method::Sync, AttemptFailure::NotHeld, now);
        let mut pick = |failures: u16| {
            missing.failures = failures;
            pick_sync_peer(&missing, &[slow, fast], &scores, now, &limits)
        };

        assert_eq!(pick(0), NextAttempt::Sync { peer: fast });
        assert_eq!(pick(1), NextAttempt::Sync { peer: slow });
        assert_eq!(pick(2), NextAttempt::Sync { peer: fast });
    }

    // each failure of a track moves its recovery on through the free peers
    // in speed order, so a position's second peer gets its turn
    #[test]
    fn recovery_moves_on() {
        let scores = PeerScores::default();
        let now = secs(NOW);
        let limits = limits();
        let at = |position: usize, byte: u8| {
            PositionPeer { position, peer: address(byte) }
        };
        let peers = [at(1, 1), at(1, 2), at(2, 3), at(3, 4)];
        let mut missing = tried(Method::Recover, AttemptFailure::NotHeld, now);

        let first = can_recover(&missing, &peers, &scores, SMALL, now, &limits);
        missing.failures = 1;
        let next = can_recover(&missing, &peers, &scores, SMALL, now, &limits);

        let owner = vec![peers[0], peers[2], peers[3]];
        assert_eq!(first, NextAttempt::Recover { peers: owner });
        let holder = vec![peers[1], peers[2], peers[3]];
        assert_eq!(next, NextAttempt::Recover { peers: holder });
    }
}
