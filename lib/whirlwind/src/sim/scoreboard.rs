//! The persistent local scoreboard, the core of the Whirlwind signal.
//!
//! Every observer keeps its own per-target record, and the unit of record is the
//! known success certificate. Certificates circulate well before the boundary
//! acts, so folding one in at round end already gives the record a late
//! certificate would have produced. The map is created once and threaded through
//! every round of every epoch without a reset, which is what makes a score
//! survive committee churn and keeps the mechanism a local signal rather than a
//! shared tally.

use std::collections::HashMap;

use serde::Serialize;
use tape_core::types::NodeId;

/// One observer's local record about one target.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct PeerScore {
    /// Challenges this observer had the chance to score for the target.
    pub opportunities: u64,
    /// Challenges with a known success certificate.
    pub successes: u64,
    /// Length of the current unbroken run of missed challenges.
    pub consecutive_misses: u64,
}

impl PeerScore {
    /// Fold one round's certificate outcome into the record.
    pub fn record(&mut self, certified: bool) {
        self.opportunities += 1;
        if certified {
            self.successes += 1;
            self.consecutive_misses = 0;
        } else {
            self.consecutive_misses += 1;
        }
    }

    /// Fraction of opportunities witnessed, guarded against a zero denominator.
    pub fn success_rate(&self) -> f64 {
        self.successes as f64 / self.opportunities.max(1) as f64
    }

    /// Whether this record trips the local eviction rule, and by which path.
    ///
    /// The consecutive-miss path is checked first, then the rate-floor path.
    pub fn eviction_fires(&self, rule: &EvictionRule) -> Option<EvictionReason> {
        if self.consecutive_misses >= rule.max_consecutive {
            return Some(EvictionReason::ConsecutiveMisses);
        }
        if self.opportunities >= rule.min_opportunities && self.success_rate() < rule.rate_floor {
            return Some(EvictionReason::RateBelowFloor);
        }
        None
    }
}

/// The local rule that fires an eviction vote, matching the stated node rule.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct EvictionRule {
    /// Minimum opportunities before the rate path can fire.
    pub min_opportunities: u64,
    /// Success rate below which the rate path fires.
    pub rate_floor: f64,
    /// Consecutive misses at which the miss path fires.
    pub max_consecutive: u64,
}

impl Default for EvictionRule {
    fn default() -> Self {
        Self {
            min_opportunities: 4,
            rate_floor: 0.5,
            max_consecutive: 3,
        }
    }
}

/// Why a local rule fired for a target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum EvictionReason {
    RateBelowFloor,
    ConsecutiveMisses,
}

impl EvictionReason {
    /// Short label for reports and the event log.
    pub fn label(&self) -> &'static str {
        match self {
            EvictionReason::RateBelowFloor => "rate below floor",
            EvictionReason::ConsecutiveMisses => "consecutive misses",
        }
    }
}

/// Every observer's local per-target scores, keyed observer then target.
///
/// Created once and never cleared between epochs, and never storing a score
/// where observer equals target.
#[derive(Default)]
pub struct Scoreboard {
    scores: HashMap<NodeId, HashMap<NodeId, PeerScore>>,
}

impl Scoreboard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one round's certificate outcome for the observer about the target.
    pub fn record(&mut self, observer: NodeId, target: NodeId, certified: bool) {
        if observer == target {
            return;
        }
        self.scores
            .entry(observer)
            .or_default()
            .entry(target)
            .or_default()
            .record(certified);
    }

    /// The observer's current record about the target, if one exists.
    pub fn score(&self, observer: NodeId, target: NodeId) -> Option<&PeerScore> {
        self.scores.get(&observer).and_then(|row| row.get(&target))
    }

    /// Aggregate certified rate for a target across every observer that scored it.
    pub fn certified_rate(&self, target: NodeId) -> Option<f64> {
        let mut opportunities = 0u64;
        let mut successes = 0u64;
        for row in self.scores.values() {
            if let Some(score) = row.get(&target) {
                opportunities += score.opportunities;
                successes += score.successes;
            }
        }
        (opportunities > 0).then(|| successes as f64 / opportunities as f64)
    }

    /// Iterate every stored score as observer, target, record triples.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, NodeId, &PeerScore)> {
        self.scores.iter().flat_map(|(observer, row)| {
            row.iter().map(move |(target, score)| (*observer, *target, score))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eviction_rule() {
        let rule = EvictionRule::default();

        let mut rate = PeerScore::default();
        rate.record(true);
        rate.record(false);
        rate.record(false);
        rate.record(true);
        rate.record(false);
        assert_eq!(rate.eviction_fires(&rule), Some(EvictionReason::RateBelowFloor));

        let mut run = PeerScore::default();
        run.record(false);
        run.record(false);
        run.record(false);
        assert_eq!(run.eviction_fires(&rule), Some(EvictionReason::ConsecutiveMisses));

        let mut clean = PeerScore::default();
        for _ in 0..4 {
            clean.record(true);
        }
        assert_eq!(clean.eviction_fires(&rule), None);
    }

    #[test]
    fn no_self_scores() {
        let mut board = Scoreboard::new();
        board.record(NodeId(1), NodeId(1), true);
        assert!(board.score(NodeId(1), NodeId(1)).is_none());
    }

    #[test]
    fn record_run() {
        let mut board = Scoreboard::new();
        for _ in 0..3 {
            board.record(NodeId(0), NodeId(1), false);
        }
        board.record(NodeId(0), NodeId(1), true);
        let score = board.score(NodeId(0), NodeId(1)).copied().expect("a score for the pair");
        assert_eq!(score.opportunities, 4);
        assert_eq!(score.successes, 1);
        assert_eq!(score.consecutive_misses, 0);
    }
}
