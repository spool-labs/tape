use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tape_core::spooler::GroupIndex;
use tape_retry::{Backoff, RetryConfig};

/// Delay before a repeat realign, so a node whose group really has gone quiet
/// does not re-read the chain on every round it judges.
const REPEAT_BACKOFF: RetryConfig = RetryConfig {
    base_delay: Duration::from_secs(5),
    max_delay: Duration::from_secs(300),
    max_retries: None,
};

/// One settled round's verdict on the peers this node judged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Judgement {
    pub peers: u64,
    pub certified: u64,
}

impl Judgement {
    /// A round where this node judged peers and not one of them stood.
    pub fn is_blank(&self) -> bool {
        self.peers > 0 && self.certified == 0
    }
}

/// Suspends judging when this node's view stops agreeing with its group.
///
/// A node whose in-memory view has drifted refuses every honest answer and
/// records a miss against every peer, which is indistinguishable from the whole
/// group failing at once. Blank rounds in a row are read as the view being
/// wrong, not the group.
pub struct Tripwire {
    threshold: u64,
    inner: Mutex<TripwireState>,
}

struct TripwireState {
    // Counted per group. A node seated in several groups settles a round in each
    // of them, and one healthy group must not clear another group's run.
    blank_rounds: HashMap<GroupIndex, u64>,
    is_realigning: bool,
    rounds: Arm,
    divergence: Arm,
}

/// How urgent the next realign from one source is.
struct Arm {
    has_tripped: bool,
    backoff: Backoff,
}

impl Arm {
    fn new() -> Self {
        Self {
            has_tripped: false,
            backoff: Backoff::new(REPEAT_BACKOFF),
        }
    }

    /// First trip is immediate, a repeat waits out a jittered backoff.
    fn fire(&mut self) -> Duration {
        match self.has_tripped {
            false => {
                self.has_tripped = true;
                Duration::ZERO
            }
            true => self.backoff.next_delay().unwrap_or(REPEAT_BACKOFF.max_delay),
        }
    }

    fn reset(&mut self) {
        self.has_tripped = false;
        self.backoff.reset();
    }
}

impl Tripwire {
    pub fn new(threshold: u64) -> Self {
        Self {
            threshold: threshold.max(1),
            inner: Mutex::new(TripwireState {
                blank_rounds: HashMap::new(),
                is_realigning: false,
                rounds: Arm::new(),
                divergence: Arm::new(),
            }),
        }
    }

    /// Whether judging and attesting are suspended pending a realign
    pub fn is_realigning(&self) -> bool {
        self.lock().is_realigning
    }

    pub fn blank_rounds(&self, group: GroupIndex) -> u64 {
        self.lock().blank_rounds.get(&group).copied().unwrap_or_default()
    }

    /// Feeds one group's settled round, returning the delay to realign after
    ///
    /// A clean round clears that group's run and makes the next trip from any
    /// source urgent again: a node judging successfully is not the node whose
    /// view is under suspicion.
    pub fn record_round(&self, group: GroupIndex, judgement: Judgement) -> Option<Duration> {
        let mut inner = self.lock();

        if !judgement.is_blank() {
            inner.blank_rounds.remove(&group);
            inner.rounds.reset();
            inner.divergence.reset();
            return None;
        }

        let run = inner.blank_rounds.entry(group).or_default();
        *run += 1;
        let reached = *run >= self.threshold;
        if inner.is_realigning || !reached {
            return None;
        }

        inner.blank_rounds.clear();
        inner.is_realigning = true;
        Some(inner.rounds.fire())
    }

    /// Trips on evidence from outside the round path
    ///
    /// Returns nothing when a realign is already running, so a burst of reports
    /// costs one read of the chain rather than one each. It keeps its own
    /// backoff: a disagreement the chain does not resolve would otherwise fire
    /// a fresh scan the moment the last one released.
    pub fn trip(&self) -> Option<Duration> {
        let mut inner = self.lock();
        if inner.is_realigning {
            return None;
        }

        inner.blank_rounds.clear();
        inner.is_realigning = true;
        Some(inner.divergence.fire())
    }

    /// The realign finished, whatever it found; judging resumes
    pub fn settled(&self) {
        let mut inner = self.lock();
        inner.is_realigning = false;
        inner.blank_rounds.clear();
    }

    fn lock(&self) -> MutexGuard<'_, TripwireState> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: GroupIndex = GroupIndex(1);
    const TWO: GroupIndex = GroupIndex(2);

    fn blank() -> Judgement {
        Judgement { peers: 19, certified: 0 }
    }

    fn clean() -> Judgement {
        Judgement { peers: 19, certified: 4 }
    }

    // a round that judged nobody says nothing about the view, so it is neither
    // blank nor clean
    #[test]
    fn empty_round_is_not_blank() {
        let nobody = Judgement { peers: 0, certified: 0 };
        assert!(!nobody.is_blank());
        assert!(!clean().is_blank());
        assert!(blank().is_blank());
    }

    // the run has to reach the threshold, and the first trip fires at once
    #[test]
    fn trips_at_threshold() {
        let tripwire = Tripwire::new(3);

        assert_eq!(tripwire.record_round(ONE, blank()), None);
        assert_eq!(tripwire.record_round(ONE, blank()), None);
        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
        assert!(tripwire.is_realigning());
    }

    // a node seated in two groups settles a round in each, and a run counted
    // across both would trip at half the threshold
    #[test]
    fn runs_are_per_group() {
        let tripwire = Tripwire::new(3);

        for _ in 0..2 {
            assert_eq!(tripwire.record_round(ONE, blank()), None);
            assert_eq!(tripwire.record_round(TWO, blank()), None);
        }

        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.blank_rounds(TWO), 2);
        assert!(!tripwire.is_realigning());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
    }

    // and one group still certifying must not clear another group's run, or a
    // node seated in several groups never trips at all
    #[test]
    fn a_healthy_group_does_not_mask_a_blank_one() {
        let tripwire = Tripwire::new(3);

        for _ in 0..2 {
            tripwire.record_round(ONE, blank());
            assert_eq!(tripwire.record_round(TWO, clean()), None);
        }

        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
    }

    // one round where a peer stood clears that group's run
    #[test]
    fn clean_round_resets() {
        let tripwire = Tripwire::new(3);

        tripwire.record_round(ONE, blank());
        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, clean()), None);
        assert_eq!(tripwire.blank_rounds(ONE), 0);

        assert_eq!(tripwire.record_round(ONE, blank()), None);
        assert_eq!(tripwire.record_round(ONE, blank()), None);
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
    }

    // nothing trips again while a realign is still running
    #[test]
    fn holds_while_realigning() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));

        for _ in 0..5 {
            assert_eq!(tripwire.record_round(ONE, blank()), None);
        }
        assert_eq!(tripwire.trip(), None);
        assert!(tripwire.is_realigning());
    }

    // a realign that did not fix it must not put the node back on the chain
    // every round, so the repeat waits
    #[test]
    fn repeat_backs_off() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
        tripwire.settled();
        assert!(!tripwire.is_realigning());

        tripwire.record_round(ONE, blank());
        let delay = tripwire.record_round(ONE, blank()).expect("second trip");
        assert!(delay > Duration::ZERO);
        assert!(delay <= REPEAT_BACKOFF.max_delay);
    }

    // a clean round between trips makes the next one urgent again
    #[test]
    fn clean_round_clears_the_backoff() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
        tripwire.settled();

        assert_eq!(tripwire.record_round(ONE, clean()), None);
        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
    }

    // a zero threshold would realign on the first blank round of every epoch
    #[test]
    fn threshold_has_a_floor() {
        let tripwire = Tripwire::new(0);
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
    }

    // evidence off the round path arms the same suspension, and a burst of it
    // costs one read rather than one each
    #[test]
    fn trips_from_outside() {
        let tripwire = Tripwire::new(8);

        assert_eq!(tripwire.trip(), Some(Duration::ZERO));
        assert_eq!(tripwire.trip(), None);
        assert!(tripwire.is_realigning());
    }

    // a disagreement the chain does not settle would otherwise fire a fresh
    // scan the instant the last one released
    #[test]
    fn divergence_backs_off_on_its_own() {
        let tripwire = Tripwire::new(8);

        assert_eq!(tripwire.trip(), Some(Duration::ZERO));
        tripwire.settled();

        let second = tripwire.trip().expect("second trip");
        assert!(second > Duration::ZERO);
        tripwire.settled();

        let third = tripwire.trip().expect("third trip");
        assert!(third > Duration::ZERO);
    }

    // the two sources escalate separately, so blank rounds do not spend the
    // divergence backoff and leave a real disagreement waiting five minutes
    #[test]
    fn the_two_arms_are_independent() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(ONE, blank());
        assert_eq!(tripwire.record_round(ONE, blank()), Some(Duration::ZERO));
        tripwire.settled();

        assert_eq!(tripwire.trip(), Some(Duration::ZERO));
    }
}
