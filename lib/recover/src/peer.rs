use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;

use crate::limits::SpoolLimits;
use crate::time::WallTime;

/// Share of a peer's old speed a new measurement keeps
const SPEED_KEPT: BasisPoints = BasisPoints(8_000);

/// Share of a peer's speed a new measurement brings
const SPEED_NEW: BasisPoints = BasisPoints(2_000);

/// Milliseconds in a second, for a rate in bytes per second
const MILLIS_PER_SECOND: u64 = 1_000;

/// Every peer_score this node keeps
#[derive(Debug, Default)]
pub struct PeerScores {
    scores: HashMap<Address, PeerScore>,
}

/// A peer as the turn sees it right now
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Not resting, and serving fewer of our fetches than it may
    Free { speed: Option<u64> },
    /// Serving as many of our fetches as it may
    Busy,
    /// Resting after a failure
    Resting,
    /// Failing long enough to count as gone; a probe may go once rested
    Gone { can_probe: bool },
}

/// How a peer did in one fetch
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerOutcome {
    /// It answered, with these bytes in this time
    Worked { bytes: StorageUnits, elapsed: Duration },
    /// It couldn't be reached
    Down,
    /// It ran past the fetch's time limit
    TooSlow,
    /// It refused, and may have asked for a longer wait than its rest
    Refused { wait: Option<Duration> },
    /// It sent bytes that failed their check
    WrongData,
}

impl PeerScores {
    /// A peer's status now; a peer we know nothing of is free
    pub fn get_peer_status(
        &self,
        peer: Address,
        now: WallTime,
        limits: &SpoolLimits,
    ) -> PeerStatus {
        match self.scores.get(&peer) {
            Some(score) => score.get_status(now, limits),
            None => PeerStatus::Free { speed: None },
        }
    }

    /// One of our fetches to the peer started
    pub fn start_fetch(&mut self, peer: Address) {
        let score = self.scores.entry(peer).or_insert_with(PeerScore::new);
        score.fetches_running += 1;
    }

    /// One of our fetches to the peer ended
    pub fn finish_fetch(&mut self, peer: Address) {
        if let Some(score) = self.scores.get_mut(&peer) {
            score.fetches_running = score.fetches_running.saturating_sub(1);
        }
    }

    /// Update the peer's score with how it did in a fetch
    pub fn record_peer_outcome(
        &mut self,
        peer: Address,
        outcome: PeerOutcome,
        now: WallTime,
        limits: &SpoolLimits,
    ) {
        let score = self.scores.entry(peer).or_insert_with(PeerScore::new);
        match outcome {
            PeerOutcome::Worked { bytes, elapsed } => {
                score.record_worked(bytes, elapsed, now);
            }
            PeerOutcome::Down | PeerOutcome::TooSlow => {
                score.record_failed(None, now, limits);
            }
            PeerOutcome::Refused { wait } => {
                score.record_failed(wait, now, limits);
            }
            PeerOutcome::WrongData => score.record_wrong_data(now, limits),
        }
    }

    /// Forget every peer outside `kept`
    pub fn forget_peers_except(&mut self, kept: &HashSet<Address>) {
        let is_kept = |peer: &Address, _: &mut PeerScore| kept.contains(peer);
        self.scores.retain(is_kept);
    }
}

impl PeerStatus {
    /// Whether a fetch may go to the peer now
    pub fn is_free(self) -> bool {
        matches!(self, Self::Free { .. })
    }

    /// Whether the peer rests now
    pub fn is_resting(self) -> bool {
        matches!(self, Self::Resting | Self::Gone { can_probe: false })
    }

    /// Whether the peer has been failing long enough to count as gone
    pub fn is_gone(self) -> bool {
        matches!(self, Self::Gone { .. })
    }

    /// Whether a probe may go to the gone peer now
    pub fn can_probe(self) -> bool {
        matches!(self, Self::Gone { can_probe: true })
    }

    /// The speed a free peer ranks by; one never measured counts as
    /// fastest, so it gets a try
    pub fn get_speed_rank(self) -> u64 {
        match self {
            Self::Free { speed: Some(speed) } => speed,
            Self::Free { speed: None } => u64::MAX,
            Self::Busy | Self::Resting | Self::Gone { .. } => 0,
        }
    }
}

/// What we know about one peer
#[derive(Clone, Copy, Debug)]
struct PeerScore {
    speed: Option<u64>,
    resting_until: WallTime,
    failing_since: Option<WallTime>,
    failures_in_a_row: u32,
    fetches_running: usize,
}

impl PeerScore {
    /// A peer that hasn't served us yet
    fn new() -> Self {
        Self {
            speed: None,
            resting_until: WallTime(0),
            failing_since: None,
            failures_in_a_row: 0,
            fetches_running: 0,
        }
    }

    fn get_status(&self, now: WallTime, limits: &SpoolLimits) -> PeerStatus {
        let is_resting = now < self.resting_until;
        if self.is_gone(now, limits) {
            let can_probe = !is_resting && self.fetches_running == 0;
            return PeerStatus::Gone { can_probe };
        }
        if is_resting {
            return PeerStatus::Resting;
        }
        if self.fetches_running >= limits.max_peer_fetches {
            return PeerStatus::Busy;
        }
        PeerStatus::Free { speed: self.speed }
    }

    fn is_gone(&self, now: WallTime, limits: &SpoolLimits) -> bool {
        let Some(since) = self.failing_since else {
            return false;
        };
        now.get_time_since(since) >= limits.peer_gone
    }

    fn record_worked(
        &mut self,
        bytes: StorageUnits,
        elapsed: Duration,
        now: WallTime,
    ) {
        if !bytes.is_zero() {
            self.record_speed(bytes, elapsed);
        }
        self.failures_in_a_row = 0;
        self.failing_since = None;
        self.resting_until = now;
    }

    fn record_speed(&mut self, bytes: StorageUnits, elapsed: Duration) {
        let millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let rate = bytes.as_u64().saturating_mul(MILLIS_PER_SECOND);
        let rate = rate / millis.max(1);
        let speed = match self.speed {
            Some(old) => get_speed_average(old, rate),
            None => rate,
        };
        self.speed = Some(speed);
    }

    fn record_failed(
        &mut self,
        wait: Option<Duration>,
        now: WallTime,
        limits: &SpoolLimits,
    ) {
        self.failures_in_a_row = self.failures_in_a_row.saturating_add(1);
        self.failing_since.get_or_insert(now);
        let rest = limits.get_peer_rest(self.failures_in_a_row);
        self.resting_until = now + rest.max(wait.unwrap_or_default());
    }

    fn record_wrong_data(&mut self, now: WallTime, limits: &SpoolLimits) {
        self.failing_since.get_or_insert(now);
        self.resting_until = now + limits.wrong_data_rest;
    }
}

/// The old speed's kept share plus the new measurement's share
fn get_speed_average(old: u64, new: u64) -> u64 {
    let kept = old.saturating_mul(SPEED_KEPT.0);
    let added = new.saturating_mul(SPEED_NEW.0);
    kept.saturating_add(added) / BasisPoints::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn secs(count: u64) -> WallTime {
        WallTime(count * 1_000)
    }

    fn worked(bytes: u64) -> PeerOutcome {
        let elapsed = Duration::from_secs(1);
        PeerOutcome::Worked { bytes: StorageUnits(bytes), elapsed }
    }

    // a failing peer rests 5 s, doubled per failure, and a success clears it
    #[test]
    fn rest_doubles() {
        let mut scores = PeerScores::default();
        let peer = address(1);
        let limits = SpoolLimits::default();

        for _ in 0..3 {
            scores.record_peer_outcome(peer, PeerOutcome::Down, secs(0), &limits);
        }
        let resting = scores.get_peer_status(peer, secs(19), &limits);
        let rested = scores.get_peer_status(peer, secs(20), &limits);
        scores.record_peer_outcome(peer, worked(1_000), secs(20), &limits);

        assert_eq!(resting, PeerStatus::Resting);
        assert!(rested.is_free());
        let free = PeerStatus::Free { speed: Some(1_000) };
        assert_eq!(scores.get_peer_status(peer, secs(20), &limits), free);
    }

    // a peer failing 10 min straight is gone, and takes a probe once rested
    #[test]
    fn gone_peer() {
        let mut scores = PeerScores::default();
        let peer = address(1);
        let limits = SpoolLimits::default();
        scores.record_peer_outcome(peer, PeerOutcome::TooSlow, secs(0), &limits);

        let probe = scores.get_peer_status(peer, secs(600), &limits);
        scores.start_fetch(peer);
        let serving = scores.get_peer_status(peer, secs(600), &limits);

        assert_eq!(probe, PeerStatus::Gone { can_probe: true });
        assert_eq!(serving, PeerStatus::Gone { can_probe: false });
    }

    // wrong data rests a peer 30 min, and a refusal waits as long as it asks
    #[test]
    fn long_rests() {
        let mut scores = PeerScores::default();
        let (wrong, refused) = (address(1), address(2));
        let limits = SpoolLimits::default();
        let wait = Some(Duration::from_secs(60));

        let wrong_data = PeerOutcome::WrongData;
        scores.record_peer_outcome(wrong, wrong_data, secs(0), &limits);
        let refusal = PeerOutcome::Refused { wait };
        scores.record_peer_outcome(refused, refusal, secs(0), &limits);

        let status = |peer, at| scores.get_peer_status(peer, secs(at), &limits);
        assert_eq!(status(wrong, 1_799), PeerStatus::Gone { can_probe: false });
        assert_eq!(status(wrong, 1_800), PeerStatus::Gone { can_probe: true });
        assert_eq!(status(refused, 59), PeerStatus::Resting);
        assert!(status(refused, 60).is_free());
    }

    // speed keeps 80% of the old and takes 20% of the new
    #[test]
    fn speed_average() {
        let mut scores = PeerScores::default();
        let peer = address(1);
        let limits = SpoolLimits::default();

        scores.record_peer_outcome(peer, worked(1_000), secs(0), &limits);
        scores.record_peer_outcome(peer, worked(6_000), secs(0), &limits);

        let status = scores.get_peer_status(peer, secs(0), &limits);
        assert_eq!(status, PeerStatus::Free { speed: Some(2_000) });
    }

    // an answer with nothing in it ends the peer's rest and keeps its speed
    #[test]
    fn empty_answer() {
        let mut scores = PeerScores::default();
        let peer = address(1);
        let limits = SpoolLimits::default();

        scores.record_peer_outcome(peer, worked(1_000), secs(0), &limits);
        scores.record_peer_outcome(peer, PeerOutcome::Down, secs(0), &limits);
        scores.record_peer_outcome(peer, worked(0), secs(1), &limits);

        let status = scores.get_peer_status(peer, secs(1), &limits);
        assert_eq!(status, PeerStatus::Free { speed: Some(1_000) });
    }

    // a peer serving as many fetches as it may is busy, one never seen is free
    #[test]
    fn busy_peer() {
        let mut scores = PeerScores::default();
        let peer = address(1);
        let limits = SpoolLimits::default();

        let unseen = scores.get_peer_status(peer, secs(0), &limits);
        scores.start_fetch(peer);
        scores.start_fetch(peer);

        assert_eq!(unseen, PeerStatus::Free { speed: None });
        let busy = scores.get_peer_status(peer, secs(0), &limits);
        assert_eq!(busy, PeerStatus::Busy);
    }
}
