//! What one owner remembers about a peer's answers.
//!
//! The record is local and private: it is this node's own history of a peer, not
//! anything the network agrees on. A missing answer is a local miss rather than
//! proof of failure, since an honest response can be late or lost, so the record
//! accumulates and only a sustained pattern means anything.
//!
//! It is never reset at an epoch boundary. A node that stops answering keeps
//! accumulating across the boundary, which is what lets a rule fire on the next
//! one rather than starting over each time.
//!
//! Outcomes do not always arrive in round order: a certificate folds the moment
//! it forms, while a miss folds only when the next round opens, so a fold is
//! judged against what is already recorded for that round rather than against a
//! high-water mark. A late certificate may replace a recorded miss, which the
//! paper requires; nothing ever replaces a recorded success.

use serde::{Deserialize, Serialize};

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::types::{BasisPoints, EpochNumber, RoundNumber};

/// Rounds a peer must have been asked before its rate means anything.
pub const MIN_OPPORTUNITIES: u64 = 4;

/// Success rate below which a peer with enough opportunities has failed the rule.
pub const RATE_FLOOR: BasisPoints = BasisPoints(5_000);

/// Consecutive misses that fire the rule regardless of rate.
///
/// The fastest whole-node detection, at three rounds. Lower risks honest noise;
/// the measured honest certificate rate at a two-slot deadline is 100%, so the
/// noise this has to tolerate is zero.
pub const MAX_CONSECUTIVE_MISSES: u64 = 3;

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
    /// repetition alone. A success is never downgraded; a recorded miss is
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
        self.consecutive_misses >= MAX_CONSECUTIVE_MISSES
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

#[cfg(test)]
mod tests {
    use super::*;

    fn at(round: u64) -> (EpochNumber, RoundNumber) {
        (EpochNumber(1), RoundNumber(round))
    }

    // a record with nothing in it accuses nobody
    #[test]
    fn fresh_record() {
        let record = PeerRecord::default();
        assert!(!record.eviction_fires());
        assert_eq!(record.success_rate(), BasisPoints(BasisPoints::MAX));
    }

    // a peer that answers every round never trips either arm of the rule
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

    // the fast arm catches a silent node in three rounds, long before its
    // lifetime rate has moved
    #[test]
    fn run_of_misses() {
        let mut record = PeerRecord::default();
        for round in 0..100 {
            record.record(EpochNumber(1), RoundNumber(round), true, None);
        }
        for round in 100..103 {
            record.record(EpochNumber(1), RoundNumber(round), false, None);
        }

        assert!(record.success_rate() > RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    // one miss is not evidence: an honest answer can be late or lost, and a
    // single gap has to be survivable or weather evicts the network
    #[test]
    fn single_miss() {
        let mut record = PeerRecord::default();
        for round in 0..10 {
            record.record(EpochNumber(1), RoundNumber(round), round != 5, None);
        }
        assert!(!record.eviction_fires());
    }

    // a peer answering every other round never reaches three misses in a row,
    // so only the rate arm can catch it
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

    // the strip reads oldest first, and a fresh peer draws a short row rather
    // than a wall of misses it never earned
    #[test]
    fn recent_strip() {
        let mut record = PeerRecord::default();
        assert!(record.recent_rounds().is_empty());

        for (round, proved) in [true, true, false, true].into_iter().enumerate() {
            record.record(EpochNumber(1), RoundNumber(round as u64), proved, None);
        }
        assert_eq!(record.recent_rounds(), vec![true, true, false, true]);
    }

    // the strip holds a fixed number of rounds and the oldest ages out of it
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

    // a rebuild sees only the rounds the store still holds, so the strip gets
    // shorter rather than drawing the pruned ones as misses the peer never made
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

    /// Answer or miss in the given pattern, repeated until `rounds` are used.
    fn run(pattern: &[bool], rounds: u64) -> PeerRecord {
        let mut record = PeerRecord::default();
        for round in 0..rounds {
            let answered = pattern[(round as usize) % pattern.len()];
            record.record(EpochNumber(1), RoundNumber(round), answered, None);
        }
        record
    }

    // going quiet mid-epoch is the ordinary failure, and the consecutive arm
    // catches it three rounds later without waiting for the epoch to end
    #[test]
    fn stops_partway() {
        let mut record = run(&[true], 40);
        assert!(!record.eviction_fires());

        for round in 40..42 {
            record.record(EpochNumber(1), RoundNumber(round), false, None);
            assert!(!record.eviction_fires(), "fired after {} misses", round - 39);
        }
        record.record(EpochNumber(1), RoundNumber(42), false, None);
        assert!(record.eviction_fires(), "three misses in a row should fire");
    }

    // the boundary of the rule: alternating never reaches three in a row, and a
    // rate of exactly half does not clear a floor of half, so the node keeps its seat
    #[test]
    fn flapping_survives() {
        let record = run(&[true, false], 400);

        assert_eq!(record.consecutive_misses, 1);
        assert_eq!(record.success_rate(), RATE_FLOOR);
        assert!(!record.eviction_fires());
    }

    // the arms are separable, because only the run claims the peer stopped
    // answering and only that claim is one a live probe can refute
    #[test]
    fn arms_apart() {
        let mut stopped = run(&[true], 40);
        for round in 40..43 {
            stopped.record(EpochNumber(1), RoundNumber(round), false, None);
        }
        assert!(stopped.run_fires());
        assert!(!stopped.rate_fires(), "a stopped peer's lifetime rate is still good");

        let erratic = run(&[true, false, false], 60);
        assert!(!erratic.run_fires(), "one answer in three never reaches three in a row");
        assert!(erratic.rate_fires());
    }

    // two misses for every answer never reaches three in a row either, so the
    // rate arm is the only thing that catches it
    #[test]
    fn under_half() {
        let record = run(&[true, false, false], 60);

        assert!(record.consecutive_misses < MAX_CONSECUTIVE_MISSES);
        assert!(record.success_rate() < RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    // two misses then an answer clears the run, so a brief outage that ends
    // before the third round costs nothing
    #[test]
    fn recovers() {
        let mut record = run(&[true], 20);
        record.record(EpochNumber(1), RoundNumber(20), false, None);
        record.record(EpochNumber(1), RoundNumber(21), false, None);
        record.record(EpochNumber(1), RoundNumber(22), true, None);

        assert_eq!(record.consecutive_misses, 0);
        assert!(!record.eviction_fires());
    }

    // answering once every three rounds dodges the consecutive arm forever, and
    // the rate arm is what closes that gap
    #[test]
    fn resets_the_run() {
        let record = run(&[true, false, false], 300);
        assert!(record.eviction_fires());
    }

    // judging one round twice manufactures no opportunities, or a peer could be
    // evicted by repetition rather than by its answers
    #[test]
    fn replayed_round() {
        let mut record = PeerRecord::default();
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, None), Fold::Advanced);
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, Some(false)), Fold::Ignored);

        assert_eq!(record.opportunities, 1);
        assert_eq!(record.consecutive_misses, 1);
    }

    // a recorded success is never downgraded to a miss
    #[test]
    fn success_stands() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(7), true, None);
        assert_eq!(record.record(EpochNumber(1), RoundNumber(7), false, Some(true)), Fold::Ignored);

        assert_eq!(record.opportunities, 1);
        assert_eq!(record.successes, 1);
        assert_eq!(record.consecutive_misses, 0);
    }

    // certificates keep circulating after their round, so one that lands after
    // the miss was recorded replaces it
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

    // a miss settles a full round late, so a certificate for the next round can
    // land first, and the miss behind it still counts
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

    // rebuilding the strip from stored rounds lands where folding in order did
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

    // the record never resets, so a node that goes quiet late in one epoch is
    // caught early in the next rather than starting over
    #[test]
    fn across_epochs() {
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(9), false, None);
        record.record(EpochNumber(2), RoundNumber(0), false, None);
        record.record(EpochNumber(2), RoundNumber(1), false, None);

        assert_eq!(record.consecutive_misses, 3);
        assert!(record.eviction_fires());
    }
}
