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
    /// Epoch of the last outcome folded in.
    pub last_epoch: EpochNumber,
    /// Round of the last outcome folded in.
    pub last_round: RoundNumber,
    /// Whether any outcome has been folded in at all.
    pub started: bool,
}

impl PeerRecord {
    /// Fold in one round's outcome.
    ///
    /// Returns false when the round was already recorded, which happens whenever
    /// the same round is judged twice, and leaves the record untouched. Without
    /// this a retried challenge would count as a fresh opportunity and a single
    /// slow peer could be talked into an eviction by repetition alone.
    pub fn record(&mut self, epoch: EpochNumber, round: RoundNumber, proved: bool) -> bool {
        if self.started && (epoch, round) <= (self.last_epoch, self.last_round) {
            return false;
        }

        self.opportunities += 1;
        if proved {
            self.successes += 1;
            self.consecutive_misses = 0;
        } else {
            self.consecutive_misses += 1;
        }

        self.last_epoch = epoch;
        self.last_round = round;
        self.started = true;
        true
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

    /// Whether this history is enough to propose an eviction.
    ///
    /// Two arms, both from the paper's repeated-local-misses rule: a run of
    /// consecutive misses catches a node that stopped answering, and a rate floor
    /// over enough opportunities catches one that answers erratically.
    pub fn eviction_fires(&self) -> bool {
        if self.consecutive_misses >= MAX_CONSECUTIVE_MISSES {
            return true;
        }
        self.opportunities >= MIN_OPPORTUNITIES && self.success_rate() < RATE_FLOOR
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(round: u64) -> (EpochNumber, RoundNumber) {
        (EpochNumber(1), RoundNumber(round))
    }

    #[test]
    fn a_fresh_record_accuses_nobody() {
        let record = PeerRecord::default();
        assert!(!record.eviction_fires());
        assert_eq!(record.success_rate(), BasisPoints(BasisPoints::MAX));
    }

    #[test]
    fn an_answering_peer_never_trips_the_rule() {
        let mut record = PeerRecord::default();
        for round in 0..500 {
            let (epoch, round) = at(round);
            assert!(record.record(epoch, round, true));
        }
        assert_eq!(record.success_rate(), BasisPoints(BasisPoints::MAX));
        assert!(!record.eviction_fires());
    }

    #[test]
    fn a_run_of_misses_fires_before_the_rate_could() {
        // The fast arm: a node that goes silent is caught in three rounds, long
        // before its lifetime rate has moved.
        let mut record = PeerRecord::default();
        for round in 0..100 {
            record.record(EpochNumber(1), RoundNumber(round), true);
        }
        for round in 100..103 {
            record.record(EpochNumber(1), RoundNumber(round), false);
        }

        assert!(record.success_rate() > RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    #[test]
    fn one_miss_is_not_evidence() {
        // An honest response can be late or lost, so a single gap has to be
        // survivable or weather would evict the network.
        let mut record = PeerRecord::default();
        for round in 0..10 {
            record.record(EpochNumber(1), RoundNumber(round), round != 5);
        }
        assert!(!record.eviction_fires());
    }

    #[test]
    fn an_erratic_peer_trips_the_rate_arm() {
        // Answers every other round, so it never reaches three in a row but its
        // rate sits at the floor.
        let mut record = PeerRecord::default();
        for round in 0..40 {
            record.record(EpochNumber(1), RoundNumber(round), round % 2 == 0);
        }
        assert_eq!(record.consecutive_misses, 1);
        assert_eq!(record.success_rate(), BasisPoints(5_000));
        assert!(!record.eviction_fires());

        // One more miss tips it under the floor.
        record.record(EpochNumber(1), RoundNumber(40), false);
        assert!(record.success_rate() < RATE_FLOOR);
        assert!(record.eviction_fires());
    }

    #[test]
    fn a_replayed_round_does_not_count_twice() {
        // Judging the same round twice must not manufacture opportunities, or a
        // peer could be evicted by repetition rather than by its answers.
        let mut record = PeerRecord::default();
        assert!(record.record(EpochNumber(1), RoundNumber(7), false));
        assert!(!record.record(EpochNumber(1), RoundNumber(7), false));
        assert!(!record.record(EpochNumber(1), RoundNumber(6), false));
        assert!(!record.record(EpochNumber(0), RoundNumber(99), false));

        assert_eq!(record.opportunities, 1);
        assert_eq!(record.consecutive_misses, 1);
    }

    #[test]
    fn the_record_carries_across_an_epoch_boundary() {
        // Never reset, so a node that stops answering late in one epoch is caught
        // at the next boundary rather than starting over.
        let mut record = PeerRecord::default();
        record.record(EpochNumber(1), RoundNumber(9), false);
        record.record(EpochNumber(2), RoundNumber(0), false);
        record.record(EpochNumber(2), RoundNumber(1), false);

        assert_eq!(record.consecutive_misses, 3);
        assert!(record.eviction_fires());
    }
}
