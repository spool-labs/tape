//! What one owner remembers about a peer's answers.
//!
//! Local and private, never anything the network agrees on. A missing answer is
//! a local miss, not proof of failure, so only a sustained pattern means
//! anything. Never reset at a boundary, or a rule could never fire across one.
//!
//! Outcomes arrive out of round order, since a certificate folds when it forms
//! and a miss folds when the next round opens. A late certificate replaces a
//! recorded miss, which the paper requires. Nothing replaces a success.

use serde::{Deserialize, Serialize};

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::types::{BasisPoints, EpochNumber, RoundNumber};

/// Rounds a peer must have been asked before its rate means anything.
pub const MIN_OPPORTUNITIES: u64 = 4;

/// Success rate below which a peer with enough opportunities has failed the rule.
pub const RATE_FLOOR: BasisPoints = BasisPoints(5_000);

/// Consecutive misses that fire the rule regardless of rate; devnet honest
/// peers answer 87-99%, so a shorter run fires on a bad minute
pub const MAX_CONSECUTIVE_MISSES: u64 = 20;

/// The run threshold for sub-minute test epochs, where 20 rounds outruns the harness
const TEST_RUN_MISSES: u64 = 3;

/// Epochs at or under this many seconds judge on the test threshold
const TEST_EPOCH_SECS: u64 = 30;

/// The threshold in force, locked on the first real epoch duration seen
static RUN_MISSES: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

/// Derives the run threshold from the chain's epoch duration, once
pub fn set_run_threshold(epoch_secs: u64) {
    if epoch_secs > 0 {
        let _ = RUN_MISSES.set(if epoch_secs <= TEST_EPOCH_SECS {
            TEST_RUN_MISSES
        } else {
            MAX_CONSECUTIVE_MISSES
        });
    }
}

fn run_threshold() -> u64 {
    *RUN_MISSES.get().unwrap_or(&MAX_CONSECUTIVE_MISSES)
}

/// What folding one outcome did to the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fold {
    /// Nothing new: the outcome repeats what is recorded, or a success stands.
    Ignored,
    /// Folded in round order, counters and recency both updated here.
    Advanced,
    /// Counted, but the recency fields must be rebuilt from the stored rounds.
    Rebuild,
}

/// One peer's challenge history, as step 5 of the mechanism keeps it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct PeerRecord {
    /// Rounds this peer was challenged in and an answer was expected.
    pub opportunities: u64,
    /// Rounds it answered with a valid proof.
    pub successes: u64,
    /// Misses since its last success.
    pub consecutive_misses: u64,
    /// Epoch of the newest outcome folded in.
    pub last_epoch: EpochNumber,
    /// Round of the newest outcome folded in.
    pub last_round: RoundNumber,
    /// Whether any outcome has been folded in at all.
    pub started: bool,
    /// The most recent judged rounds, newest in the low bit, set for a success.
    ///
    /// One row of the challenge record grid: rounds across, spools down. Only
    /// rounds this peer was actually judged in take a bit, so a void round leaves
    /// the strip alone rather than reading as a miss against everyone.
    pub recent: u64,
    /// Bits of `recent` that stand for a round, rather than for nothing yet.
    ///
    /// Not `opportunities`: that counts every round for the peer's whole life,
    /// while a rebuild can only see the rounds the store still holds. Reading the
    /// strip off the lifetime count draws the pruned ones as misses.
    pub recent_len: u64,
}

/// Judged rounds the recent strip remembers.
pub const RECENT_ROUNDS: u32 = u64::BITS;

impl PeerRecord {
    /// Fold in one round's outcome, given what is already recorded for it.
    ///
    /// `prior` is the outcome the caller has stored for this exact round, so a
    /// repeat never counts twice and a peer cannot be talked into an eviction by
    /// repetition alone. A success is never downgraded. A recorded miss is
    /// upgraded when its certificate arrives late. A fold behind the newest
    /// round still counts, but the caller must rebuild recency from the stored
    /// rounds, since only they know the order.
    pub fn record(
        &mut self,
        epoch: EpochNumber,
        round: RoundNumber,
        proved: bool,
        prior: Option<bool>,
    ) -> Fold {
        match prior {
            Some(true) => return Fold::Ignored,
            Some(false) if !proved => return Fold::Ignored,
            Some(false) => {
                self.successes += 1;
                return Fold::Rebuild;
            }
            None => {}
        }

        self.opportunities += 1;
        if proved {
            self.successes += 1;
        }

        if self.started && (epoch, round) <= (self.last_epoch, self.last_round) {
            return Fold::Rebuild;
        }

        if proved {
            self.consecutive_misses = 0;
        } else {
            self.consecutive_misses += 1;
        }
        self.recent = (self.recent << 1) | u64::from(proved);
        self.recent_len = (self.recent_len + 1).min(RECENT_ROUNDS as u64);
        self.last_epoch = epoch;
        self.last_round = round;
        self.started = true;
        Fold::Advanced
    }

    /// Re-derive the order-sensitive fields from the rounds as stored.
    ///
    /// `rounds` is every recorded outcome for this peer, oldest first.
    pub fn rebuild_recency(&mut self, rounds: &[(EpochNumber, RoundNumber, bool)]) {
        self.consecutive_misses = rounds
            .iter()
            .rev()
            .take_while(|(_, _, proved)| !proved)
            .count() as u64;

        self.recent = 0;
        let tail = rounds.len().saturating_sub(RECENT_ROUNDS as usize);
        for (_, _, proved) in &rounds[tail..] {
            self.recent = (self.recent << 1) | u64::from(*proved);
        }
        self.recent_len = (rounds.len() - tail) as u64;

        if let Some((epoch, round, _)) = rounds.last() {
            self.last_epoch = *epoch;
            self.last_round = *round;
            self.started = true;
        }
    }

    /// Share of opportunities the peer answered, in basis points.
    ///
    /// A peer never asked reads as a full rate, so a fresh record never trips the
    /// rate arm before it has been asked anything.
    pub fn success_rate(&self) -> BasisPoints {
        if self.opportunities == 0 {
            return BasisPoints(BasisPoints::MAX);
        }
        BasisPoints(self.successes * BasisPoints::MAX / self.opportunities)
    }

    /// Whether a run of consecutive misses has reached the fast arm.
    ///
    /// This arm makes a claim about reachability: the peer has stopped answering.
    /// It is the one a live probe can speak to, and the record cannot check it
    /// for itself, since a run only grows while the peer is being judged and
    /// clearing it needs a success the peer has no chance to earn once the rounds
    /// have stopped.
    pub fn run_fires(&self) -> bool {
        self.consecutive_misses >= run_threshold()
    }

    /// Whether the lifetime rate has fallen through the floor.
    ///
    /// This arm makes no claim about reachability. A peer answering every round
    /// with an invalid proof trips it while staying perfectly reachable, so no
    /// amount of answering a probe clears it.
    pub fn rate_fires(&self) -> bool {
        self.opportunities >= MIN_OPPORTUNITIES && self.success_rate() < RATE_FLOOR
    }

    /// Whether this history is enough to propose an eviction.
    ///
    /// Two arms, both from the paper's repeated-local-misses rule: a run of
    /// consecutive misses catches a node that stopped answering, and a rate floor
    /// over enough opportunities catches one that answers erratically.
    pub fn eviction_fires(&self) -> bool {
        self.run_fires() || self.rate_fires()
    }

    /// The recent strip oldest-first, for drawing one row of the grid.
    ///
    /// Only the rounds still remembered, so a fresh peer reads as a short row
    /// rather than a wall of misses, and so does one whose older rounds have
    /// been pruned out from under a rebuild.
    pub fn recent_rounds(&self) -> Vec<bool> {
        let judged = self.recent_len.min(RECENT_ROUNDS as u64) as u32;
        (0..judged)
            .rev()
            .map(|bit| self.recent & (1 << bit) != 0)
            .collect()
    }
}

/// What a node's spools say about it, as one answer.
///
/// The record is per spool but a proposal is against the node, so the arms are
/// read across every spool it answers for. Kept here rather than at each caller:
/// the eviction judge, the observe board and the dashboard were each deriving
/// their own, and had already drifted apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeVerdict {
    /// No spool has been judged enough times to mean anything.
    Unproven,
    /// A spool's lifetime rate is through the floor. It says nothing about
    /// reachability, so answering a probe cannot clear it.
    RateFailed,
    /// A spool has stopped answering. A live probe can clear it.
    RunFailed,
    /// Nothing has fired.
    Healthy,
}

/// Judge a node on every spool it still answers for.
///
/// The rate arm outranks the run arm because only the run arm makes a claim a
/// probe can settle, so a node failing both is not let off by answering.
pub fn node_verdict<'a>(records: impl IntoIterator<Item = &'a PeerRecord>) -> NodeVerdict {
    let mut judged = 0;
    let mut rate = false;
    let mut run = false;
    for record in records {
        judged = judged.max(record.opportunities);
        rate |= record.rate_fires();
        run |= record.run_fires();
    }

    if rate {
        NodeVerdict::RateFailed
    } else if judged < MIN_OPPORTUNITIES {
        // Below the floor a run is thinner evidence than a live probe, and a
        // short active phase can leave a spool with only a handful of rounds.
        NodeVerdict::Unproven
    } else if run {
        NodeVerdict::RunFailed
    } else {
        NodeVerdict::Healthy
    }
}

/// A node's spools counted together, for reporting rather than for judging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeTally {
    /// Spools this node keeps a record for.
    pub spools: u64,
    /// Rounds judged, summed over them.
    pub opportunities: u64,
    /// Rounds answered, summed the same way.
    pub successes: u64,
    /// The longest run any one spool is on, which is the one that would fire.
    pub worst_run: u64,
}

impl NodeTally {
    /// Answered over judged, pooled across the spools.
    ///
    /// For display only. No arm of the rule reads a pooled rate: a node failing
    /// one spool of five still shows four fifths here while `node_verdict` has
    /// already failed it.
    pub fn rate(&self) -> BasisPoints {
        if self.opportunities == 0 {
            return BasisPoints(BasisPoints::MAX);
        }
        BasisPoints(self.successes * BasisPoints::MAX / self.opportunities)
    }
}

/// Count a node's spools together.
pub fn node_tally<'a>(records: impl IntoIterator<Item = &'a PeerRecord>) -> NodeTally {
    let mut tally = NodeTally::default();
    for record in records {
        tally.spools += 1;
        tally.opportunities += record.opportunities;
        tally.successes += record.successes;
        tally.worst_run = tally.worst_run.max(record.consecutive_misses);
    }
    tally
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spool(opportunities: u64, successes: u64, run: u64) -> PeerRecord {
        PeerRecord {
            opportunities,
            successes,
            consecutive_misses: run,
            ..PeerRecord::default()
        }
    }

    #[test]
    fn one_dead_spool_fails_the_node() {
        let healthy = spool(20, 20, 0);
        let dead = spool(20, 0, MAX_CONSECUTIVE_MISSES);
        let records = [healthy, healthy, healthy, healthy, dead];

        assert_eq!(node_verdict(&records), NodeVerdict::RateFailed);

        // The pooled rate the board shows is comfortably above the floor, which
        // is why it must never be what the rule reads.
        let tally = node_tally(&records);
        assert_eq!(tally.spools, 5);
        assert!(tally.rate() > RATE_FLOOR);
        assert_eq!(tally.worst_run, MAX_CONSECUTIVE_MISSES);
    }

    #[test]
    fn rate_outranks_run() {
        let both = spool(MIN_OPPORTUNITIES, 0, MAX_CONSECUTIVE_MISSES);
        assert!(both.rate_fires() && both.run_fires());
        assert_eq!(node_verdict(&[both]), NodeVerdict::RateFailed);
    }

    #[test]
    fn too_few_rounds_is_unproven() {
        let short = spool(MIN_OPPORTUNITIES - 1, 0, MIN_OPPORTUNITIES - 1);
        assert!(!short.rate_fires(), "too few rounds for the rate arm");
        assert!(!short.run_fires(), "too few rounds to be a run");
        assert_eq!(node_verdict(&[short]), NodeVerdict::Unproven);
    }

    #[test]
    fn a_long_run_fails_the_node() {
        let stalled = spool(MIN_OPPORTUNITIES + 4, MIN_OPPORTUNITIES + 1, MAX_CONSECUTIVE_MISSES);
        assert!(!stalled.rate_fires());
        assert_eq!(node_verdict(&[stalled]), NodeVerdict::RunFailed);
    }

    #[test]
    fn no_records_is_unproven() {
        assert_eq!(node_verdict(&[]), NodeVerdict::Unproven);
        assert_eq!(node_tally(&[]).rate(), BasisPoints(BasisPoints::MAX));
    }

    #[test]
    fn every_spool_answering_is_healthy() {
        let good = spool(MIN_OPPORTUNITIES + 2, MIN_OPPORTUNITIES + 2, 0);
        assert_eq!(node_verdict(&[good, good, good]), NodeVerdict::Healthy);
    }

    fn at(round: u64) -> (EpochNumber, RoundNumber) {
        (EpochNumber(1), RoundNumber(round))
    }

    #[test]
    fn fresh_record() {
        let record = PeerRecord::default();
        assert!(!record.eviction_fires());
        assert_eq!(record.success_rate(), BasisPoints(BasisPoints::MAX));
    }

    #[test]
    fn always_answers() {
        let mut record = PeerRecord::default();
        for round in 0..500 {
            let (epoch, round) = at(round);
            assert_eq!(record.record(epoch, round, true, None), Fold::Advanced);
        }
        assert_eq!(record.success_rate(), BasisPoints(BasisPoints::MAX));
        assert!(!record.eviction_fires());
    }

    #[test]
    fn run_of_misses() {
        let mut record = PeerRecord::default();
        for round in 0..100 {
            record.record(EpochNumber(1), RoundNumber(round), true, None);
        }
        for round in 100..100 + MAX_CONSECUTIVE_MISSES {
            record.record(EpochNumber(1), RoundNumber(round), false, None);
        }

        assert!(record.success_rate() > RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    #[test]
    fn single_miss() {
        let mut record = PeerRecord::default();
        for round in 0..10 {
            record.record(EpochNumber(1), RoundNumber(round), round != 5, None);
        }
        assert!(!record.eviction_fires());
    }

    #[test]
    fn erratic_peer() {
        let mut record = PeerRecord::default();
        for round in 0..40 {
            record.record(EpochNumber(1), RoundNumber(round), round % 2 == 0, None);
        }
        assert_eq!(record.consecutive_misses, 1);
        assert_eq!(record.success_rate(), BasisPoints(5_000));
        assert!(!record.eviction_fires());

        // One more miss tips it under the floor.
        record.record(EpochNumber(1), RoundNumber(40), false, None);
        assert!(record.success_rate() < RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    #[test]
    fn recent_strip() {
        let mut record = PeerRecord::default();
        assert!(record.recent_rounds().is_empty());

        for (round, proved) in [true, true, false, true].into_iter().enumerate() {
            record.record(EpochNumber(1), RoundNumber(round as u64), proved, None);
        }
        assert_eq!(record.recent_rounds(), vec![true, true, false, true]);
    }

    #[test]
    fn strip_ages_out() {
        let mut record = PeerRecord::default();
        for round in 0..(RECENT_ROUNDS as u64 + 10) {
            // Miss only the very first round, which falls off the end.
            record.record(EpochNumber(1), RoundNumber(round), round != 0, None);
        }

        let strip = record.recent_rounds();
        assert_eq!(strip.len(), RECENT_ROUNDS as usize);
        assert!(strip.iter().all(|proved| *proved), "an aged-out miss lingered");
        assert_eq!(record.opportunities, RECENT_ROUNDS as u64 + 10);
    }

    #[test]
    fn strip_survives_pruned_rounds() {
        let mut record = PeerRecord::default();
        for round in 0..5 {
            record.record(EpochNumber(1), RoundNumber(round), true, None);
        }

        // A late outcome behind the newest round, with everything before epoch 2
        // already swept out of the round store.
        let fold = record.record(EpochNumber(1), RoundNumber(4), true, None);
        assert_eq!(fold, Fold::Rebuild);
        record.rebuild_recency(&[at_round(2, 0, true), at_round(2, 1, true)]);

        assert_eq!(record.opportunities, 6);
        assert_eq!(record.recent_rounds(), vec![true, true]);
    }

    fn at_round(epoch: u64, round: u64, proved: bool) -> (EpochNumber, RoundNumber, bool) {
        (EpochNumber(epoch), RoundNumber(round), proved)
    }

    fn run(pattern: &[bool], rounds: u64) -> PeerRecord {
        let mut record = PeerRecord::default();
        for round in 0..rounds {
            let answered = pattern[(round as usize) % pattern.len()];
            record.record(EpochNumber(1), RoundNumber(round), answered, None);
        }
        record
    }

    #[test]
    fn stops_partway() {
        let mut record = run(&[true], 40);
        assert!(!record.eviction_fires());

        let last = 40 + MAX_CONSECUTIVE_MISSES - 1;
        for round in 40..last {
            record.record(EpochNumber(1), RoundNumber(round), false, None);
            assert!(!record.eviction_fires(), "fired after {} misses", round - 39);
        }
        record.record(EpochNumber(1), RoundNumber(last), false, None);
        assert!(record.eviction_fires(), "a full run of misses should fire");
    }

    #[test]
    fn flapping_survives() {
        let record = run(&[true, false], 400);

        assert_eq!(record.consecutive_misses, 1);
        assert_eq!(record.success_rate(), RATE_FLOOR);
        assert!(!record.eviction_fires());
    }

    #[test]
    fn arms_apart() {
        let mut stopped = run(&[true], 40);
        for round in 40..40 + MAX_CONSECUTIVE_MISSES {
            stopped.record(EpochNumber(1), RoundNumber(round), false, None);
        }
        assert!(stopped.run_fires());
        assert!(!stopped.rate_fires(), "a stopped peer's lifetime rate is still good");

        let erratic = run(&[true, false, false], 60);
        assert!(!erratic.run_fires(), "one answer in three never reaches a full run");
        assert!(erratic.rate_fires());
    }

    #[test]
    fn under_half() {
        let record = run(&[true, false, false], 60);

        assert!(record.consecutive_misses < MAX_CONSECUTIVE_MISSES);
        assert!(record.success_rate() < RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    #[test]
    fn recovers() {
        let mut record = run(&[true], 20);
        record.record(EpochNumber(1), RoundNumber(20), false, None);
        record.record(EpochNumber(1), RoundNumber(21), false, None);
        record.record(EpochNumber(1), RoundNumber(22), true, None);

        assert_eq!(record.consecutive_misses, 0);
        assert!(!record.eviction_fires());
    }

    #[test]
    fn resets_the_run() {
        let record = run(&[true, false, false], 300);
        assert!(record.eviction_fires());
    }

    #[test]
    fn replayed_round() {
        let mut record = PeerRecord::default();
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, None), Fold::Advanced);
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, Some(false)), Fold::Ignored);

        assert_eq!(record.opportunities, 1);
        assert_eq!(record.consecutive_misses, 1);
    }

    #[test]
    fn success_stands() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(7), true, None);
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, Some(true)), Fold::Ignored);

        assert_eq!(record.opportunities, 1);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 0);
    }

    #[test]
    fn late_certificate() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(7), false, None);
        record.record(EpochNumber(1), RoundNumber(8), false, None);
        assert_eq!(record.consecutive_misses, 2);

        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), true, Some(false)), Fold::Rebuild);
        record.rebuild_recency(&[
            (EpochNumber(1), RoundNumber(7), true),
            (EpochNumber(1), RoundNumber(8), false),
        ]);

        assert_eq!(record.opportunities, 2);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 1);
        assert_eq!(record.recent_rounds(), vec![true, false]);
    }

    #[test]
    fn late_miss() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(8), true, None);

        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, None), Fold::Rebuild);
        record.rebuild_recency(&[
            (EpochNumber(1), RoundNumber(7), false),
            (EpochNumber(1), RoundNumber(8), true),
        ]);

        assert_eq!(record.opportunities, 2);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 0, "a later success already broke the run");
        assert_eq!(record.recent_rounds(), vec![false, true]);
    }

    #[test]
    fn rebuild_matches() {
        let rounds: Vec<(EpochNumber, RoundNumber, bool)> = (0..100)
            .map(|round| (EpochNumber(1), RoundNumber(round), round % 3 != 0))
            .collect();

        let mut folded = PeerRecord::default();
        for (epoch, round, proved) in &rounds {
            folded.record(*epoch, *round, *proved, None);
        }

        let mut rebuilt = folded;
        rebuilt.rebuild_recency(&rounds);

        assert_eq!(rebuilt, folded);
    }

    #[test]
    fn across_epochs() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(9), false, None);
        for round in 0..MAX_CONSECUTIVE_MISSES - 1 {
            record.record(EpochNumber(2), RoundNumber(round), false, None);
        }

        assert_eq!(record.consecutive_misses, MAX_CONSECUTIVE_MISSES);
        assert!(record.eviction_fires());
    }
}
