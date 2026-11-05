//! peer_score and the peer lists the owner_history gives
//!
//! A peer_score lives in memory only. The words the turn decides by come
//! from it: free, gone, fastest and probe.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tape_core::spooler::VirtualGroup;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::address::Address;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::config::recovery::RecoveryConfig;
use crate::features::spool::error::SpoolError;

/// A rate in bytes per second
pub type BytesPerSecond = u64;

/// Share of the old speed a new measurement keeps, out of `SPEED_WHOLE`
const SPEED_KEPT: u64 = 4;

/// The whole a speed share is counted out of
const SPEED_WHOLE: u64 = 5;

/// Milliseconds in a second, for a rate
const MILLIS: u64 = 1_000;

/// The peer waits and limits a peer_score is judged by
#[derive(Clone, Copy, Debug)]
pub struct PeerRules {
    pub rest: Duration,
    pub rest_max: Duration,
    pub wrong_data_rest: Duration,
    pub gone: Duration,
    pub max_fetches: usize,
}

/// What we know about one peer
#[derive(Clone, Copy, Debug)]
pub struct PeerScore {
    /// Nothing until a fetch measured it
    pub speed: Option<BytesPerSecond>,
    pub resting_until: Instant,
    pub failing_since: Option<Instant>,
    pub failures_in_a_row: u32,
    pub fetches_running: usize,
}

/// A peer as the turn sees it right now
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Not resting and serving fewer than the most fetches
    Free { speed: Option<BytesPerSecond> },
    /// Serving as many of our fetches as it may
    Busy,
    Resting,
    /// Failing long enough to count as gone; a probe may go once rested
    Gone { can_probe: bool },
}

/// How a peer did in one fetch
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerOutcome {
    Worked { bytes: StorageUnits, elapsed: Duration },
    Down,
    TooSlow,
    /// The peer may ask us to wait longer than its rest
    Refused { wait: Option<Duration> },
    WrongData,
}

/// One peer and its status
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerView {
    pub peer: Address,
    pub status: PeerStatus,
}

/// A past owner of a spool, told apart by the epoch it owned the spool in
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Holder {
    pub node: Address,
    pub epoch: EpochNumber,
}

/// One recorded epoch's owners, by position
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochOwners {
    pub epoch: EpochNumber,
    pub owners: Vec<Address>,
}

/// Who owned a virtual group's positions, newest epoch first
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupHistory {
    pub now: EpochNumber,
    pub epochs: Vec<EpochOwners>,
}

/// Every peer_score this node keeps
#[derive(Debug, Default)]
pub struct PeerScores {
    scores: HashMap<Address, PeerScore>,
}

impl PeerRules {
    /// The peer rules a recovery config sets
    pub fn from_config(config: &RecoveryConfig) -> Self {
        Self {
            rest: Duration::from_secs(config.peer_rest_secs),
            rest_max: Duration::from_secs(config.peer_rest_max_secs),
            wrong_data_rest: Duration::from_secs(config.wrong_data_rest_secs),
            gone: Duration::from_secs(config.peer_gone_secs),
            max_fetches: config.max_peer_fetches,
        }
    }
}

impl PeerScore {
    /// A peer that hasn't served us yet
    pub fn fresh(now: Instant) -> Self {
        Self {
            speed: None,
            resting_until: now,
            failing_since: None,
            failures_in_a_row: 0,
            fetches_running: 0,
        }
    }
}

impl PeerStatus {
    /// Whether a fetch may go to the peer now
    pub fn is_free(self) -> bool {
        matches!(self, Self::Free { .. })
    }

    /// Whether the peer has been failing for long enough to count as gone
    pub fn is_gone(self) -> bool {
        matches!(self, Self::Gone { .. })
    }

    /// Whether the peer rests now
    pub fn is_resting(self) -> bool {
        matches!(self, Self::Resting | Self::Gone { can_probe: false })
    }

    /// Whether a probe may go to the gone peer now
    pub fn can_probe(self) -> bool {
        matches!(self, Self::Gone { can_probe: true })
    }

    /// The speed a free peer ranks by; never measured counts as fastest,
    /// so it gets a try
    pub fn speed_rank(self) -> BytesPerSecond {
        match self {
            Self::Free { speed: Some(speed) } => speed,
            Self::Free { speed: None } => BytesPerSecond::MAX,
            Self::Busy | Self::Resting | Self::Gone { .. } => 0,
        }
    }
}

impl PeerScores {
    /// A peer's status now; a peer we know nothing of is free
    pub fn status(
        &self,
        peer: Address,
        now: Instant,
        rules: &PeerRules,
    ) -> PeerStatus {
        match self.scores.get(&peer) {
            Some(score) => status(score, now, rules),
            None => PeerStatus::Free { speed: None },
        }
    }

    /// A fetch to the peer started
    pub fn start(&mut self, peer: Address, now: Instant) {
        let score = self.score(peer, now);
        score.fetches_running += 1;
    }

    /// A fetch to the peer ended
    pub fn release(&mut self, peer: Address, now: Instant) {
        let score = self.score(peer, now);
        score.fetches_running = score.fetches_running.saturating_sub(1);
    }

    /// Update the peer's score with how it did in a fetch
    pub fn update(
        &mut self,
        peer: Address,
        outcome: PeerOutcome,
        now: Instant,
        rules: &PeerRules,
    ) {
        let score = self.score(peer, now);
        record(score, outcome, now, rules);
    }

    /// Forget every peer outside `keep`
    pub fn keep_only(&mut self, keep: &HashSet<Address>) {
        let is_kept = |peer: &Address, _: &mut PeerScore| keep.contains(peer);
        self.scores.retain(is_kept);
    }

    fn score(&mut self, peer: Address, now: Instant) -> &mut PeerScore {
        let fresh = || PeerScore::fresh(now);
        self.scores.entry(peer).or_insert_with(fresh)
    }
}

/// A peer's status from its score
pub fn status(
    score: &PeerScore,
    now: Instant,
    rules: &PeerRules,
) -> PeerStatus {
    let is_resting = now < score.resting_until;
    if is_gone(score, now, rules) {
        let can_probe = !is_resting && score.fetches_running == 0;
        return PeerStatus::Gone { can_probe };
    }
    if is_resting {
        return PeerStatus::Resting;
    }
    if score.fetches_running >= rules.max_fetches {
        return PeerStatus::Busy;
    }
    PeerStatus::Free { speed: score.speed }
}

fn is_gone(score: &PeerScore, now: Instant, rules: &PeerRules) -> bool {
    let Some(since) = score.failing_since else {
        return false;
    };
    now.saturating_duration_since(since) >= rules.gone
}

/// Update a peer_score after a fetch
pub fn record(
    score: &mut PeerScore,
    outcome: PeerOutcome,
    now: Instant,
    rules: &PeerRules,
) {
    match outcome {
        PeerOutcome::Worked { bytes, elapsed } => {
            worked(score, bytes, elapsed, now);
        }
        PeerOutcome::Down | PeerOutcome::TooSlow => {
            failed(score, None, now, rules);
        }
        PeerOutcome::Refused { wait } => failed(score, wait, now, rules),
        PeerOutcome::WrongData => sent_wrong_data(score, now, rules),
    }
}

fn worked(
    score: &mut PeerScore,
    bytes: StorageUnits,
    elapsed: Duration,
    now: Instant,
) {
    let millis = (elapsed.as_millis() as u64).max(1);
    let rate = bytes.as_u64().saturating_mul(MILLIS) / millis;
    let speed = match score.speed {
        Some(old) => (old.saturating_mul(SPEED_KEPT) + rate) / SPEED_WHOLE,
        None => rate,
    };
    score.speed = Some(speed);
    score.failures_in_a_row = 0;
    score.failing_since = None;
    score.resting_until = now;
}

fn failed(
    score: &mut PeerScore,
    wait: Option<Duration>,
    now: Instant,
    rules: &PeerRules,
) {
    score.failures_in_a_row = score.failures_in_a_row.saturating_add(1);
    score.failing_since.get_or_insert(now);
    let rest = doubled(rules.rest, score.failures_in_a_row, rules.rest_max);
    score.resting_until = now + rest.max(wait.unwrap_or_default());
}

fn sent_wrong_data(score: &mut PeerScore, now: Instant, rules: &PeerRules) {
    score.failing_since.get_or_insert(now);
    score.resting_until = now + rules.wrong_data_rest;
}

/// `base` doubled for each count past the first, up to `max`
pub fn doubled(base: Duration, count: u32, max: Duration) -> Duration {
    let doublings = count.saturating_sub(1).min(u32::BITS - 1);
    base.saturating_mul(1 << doublings).min(max)
}

/// The holders of a position: its owners of the epochs before now, newest
/// first, each node once at its newest epoch, our node left out
pub fn holders(
    history: &GroupHistory,
    position: usize,
    me: Address,
) -> Vec<Holder> {
    let mut holders: Vec<Holder> = Vec::new();
    for owners in &history.epochs {
        if owners.epoch >= history.now {
            continue;
        }
        let Some(node) = owner_at(owners, position, me) else {
            continue;
        };
        if !has_holder(&holders, node) {
            holders.push(Holder { node, epoch: owners.epoch });
        }
    }
    holders
}

/// The peers of a position: its owner now, then its holders
pub fn position_peers(
    history: &GroupHistory,
    position: usize,
    me: Address,
) -> Vec<Address> {
    let mut peers = Vec::new();
    if let Some(owner) = owner_now(history, position, me) {
        peers.push(owner);
    }
    for holder in holders(history, position, me) {
        push_once(&mut peers, holder.node);
    }
    peers
}

/// The sync_peers of a spool: every node in the spool_group now, then the
/// spool's holders
pub fn sync_peers(
    history: &GroupHistory,
    position: usize,
    me: Address,
) -> Vec<Address> {
    let mut peers = Vec::new();
    for other in 0..width(history) {
        if let Some(owner) = owner_now(history, other, me) {
            push_once(&mut peers, owner);
        }
    }
    for holder in holders(history, position, me) {
        push_once(&mut peers, holder.node);
    }
    peers
}

fn width(history: &GroupHistory) -> usize {
    match history.epochs.first() {
        Some(owners) => owners.owners.len(),
        None => 0,
    }
}

fn owner_now(
    history: &GroupHistory,
    position: usize,
    me: Address,
) -> Option<Address> {
    let owners = history.epochs.first()?;
    if owners.epoch != history.now {
        return None;
    }
    owner_at(owners, position, me)
}

fn owner_at(
    owners: &EpochOwners,
    position: usize,
    me: Address,
) -> Option<Address> {
    let node = *owners.owners.get(position)?;
    (node != Address::default() && node != me).then_some(node)
}

fn has_holder(holders: &[Holder], node: Address) -> bool {
    for holder in holders {
        if holder.node == node {
            return true;
        }
    }
    false
}

fn push_once(peers: &mut Vec<Address>, peer: Address) {
    if !peers.contains(&peer) {
        peers.push(peer);
    }
}

/// Read a virtual group's owners from now back over the recorded epochs
pub fn load_history<Db: Store>(
    store: &TapeStore<Db>,
    now: EpochNumber,
    group: VirtualGroup,
    width: usize,
    keep_epochs: u64,
) -> Result<GroupHistory, SpoolError> {
    let mut epochs = Vec::new();
    for back in 0..=keep_epochs.min(now.as_u64()) {
        let epoch = EpochNumber(now.as_u64() - back);
        let read = store.group_owners_at(epoch, group, width);
        if let Some(owners) = read.map_err(store_error)? {
            epochs.push(EpochOwners { epoch, owners });
        }
    }
    Ok(GroupHistory { now, epochs })
}

fn store_error(error: impl std::fmt::Display) -> SpoolError {
    SpoolError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> PeerRules {
        PeerRules::from_config(&RecoveryConfig::default())
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn worked(bytes: u64) -> PeerOutcome {
        let elapsed = Duration::from_secs(1);
        PeerOutcome::Worked { bytes: StorageUnits(bytes), elapsed }
    }

    fn history(now: u64, rows: &[(u64, [u8; 3])]) -> GroupHistory {
        let mut epochs = Vec::new();
        for (epoch, bytes) in rows {
            let mut owners = Vec::new();
            for byte in bytes {
                owners.push(address(*byte));
            }
            epochs.push(EpochOwners { epoch: EpochNumber(*epoch), owners });
        }
        GroupHistory { now: EpochNumber(now), epochs }
    }

    // a failing peer rests 5 s, doubled per failure, and a success clears it
    #[test]
    fn rest_doubles() {
        let now = Instant::now();
        let mut score = PeerScore::fresh(now);

        for _ in 0..3 {
            record(&mut score, PeerOutcome::Down, now, &rules());
        }
        assert_eq!(score.resting_until - now, Duration::from_secs(20));
        assert_eq!(status(&score, now, &rules()), PeerStatus::Resting);

        record(&mut score, worked(1_000), now, &rules());
        let free = PeerStatus::Free { speed: Some(1_000) };
        assert_eq!(status(&score, now, &rules()), free);
    }

    // a peer failing 10 min straight is gone, and takes a probe once rested
    #[test]
    fn gone_peer() {
        let start = Instant::now();
        let mut score = PeerScore::fresh(start);
        record(&mut score, PeerOutcome::TooSlow, start, &rules());
        let later = start + Duration::from_secs(600);

        let probe = PeerStatus::Gone { can_probe: true };
        assert_eq!(status(&score, later, &rules()), probe);
        score.fetches_running = 1;
        let serving = PeerStatus::Gone { can_probe: false };
        assert_eq!(status(&score, later, &rules()), serving);
    }

    // wrong data rests a peer 30 min, and a refusal waits as long as it asks
    #[test]
    fn long_rests() {
        let now = Instant::now();
        let mut wrong = PeerScore::fresh(now);
        let mut refused = PeerScore::fresh(now);

        record(&mut wrong, PeerOutcome::WrongData, now, &rules());
        let wait = Some(Duration::from_secs(60));
        record(&mut refused, PeerOutcome::Refused { wait }, now, &rules());

        assert_eq!(wrong.resting_until - now, Duration::from_secs(1_800));
        assert_eq!(refused.resting_until - now, Duration::from_secs(60));
    }

    // speed keeps 80% of the old and takes 20% of the new
    #[test]
    fn speed_average() {
        let now = Instant::now();
        let mut score = PeerScore::fresh(now);

        record(&mut score, worked(1_000), now, &rules());
        record(&mut score, worked(6_000), now, &rules());

        assert_eq!(score.speed, Some(2_000));
    }

    // a peer serving as many fetches as it may is busy, one never seen is free
    #[test]
    fn busy_peer() {
        let now = Instant::now();
        let mut scores = PeerScores::default();
        let peer = address(1);

        let unseen = scores.status(peer, now, &rules());
        scores.start(peer, now);
        scores.start(peer, now);

        assert_eq!(unseen, PeerStatus::Free { speed: None });
        assert_eq!(scores.status(peer, now, &rules()), PeerStatus::Busy);
    }

    // holders come newest first, once each, without our node or the owner now
    #[test]
    fn holder_order() {
        let me = address(9);
        let rows = [
            (10, [9, 1, 1]),
            (9, [2, 1, 1]),
            (8, [9, 1, 1]),
            (7, [3, 1, 1]),
            (6, [2, 1, 1]),
        ];
        let history = history(10, &rows);

        let holders = holders(&history, 0, me);

        let expected = vec![
            Holder { node: address(2), epoch: EpochNumber(9) },
            Holder { node: address(3), epoch: EpochNumber(7) },
        ];
        assert_eq!(holders, expected);
    }

    // sync_peers holds the group now, then the spool's holders
    #[test]
    fn sync_list() {
        let me = address(9);
        let history = history(10, &[(10, [9, 4, 5]), (9, [2, 4, 5])]);

        let peers = sync_peers(&history, 0, me);
        let others = position_peers(&history, 1, me);

        assert_eq!(peers, vec![address(4), address(5), address(2)]);
        assert_eq!(others, vec![address(4)]);
    }
}
