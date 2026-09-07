use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tape_core::spooler::GroupIndex;
use tape_retry::{Backoff, RetryConfig};

/// Delay before a repeat realign, so a node whose group really has gone quiet
/// does not re-read the chain on every round it settles.
const REPEAT_BACKOFF: RetryConfig = RetryConfig {
    base_delay: Duration::from_secs(5),
    max_delay: Duration::from_secs(300),
    max_retries: None,
};

/// Suspends settlement when this node's view stops agreeing with its group.
///
/// A node whose in-memory view has drifted refuses every honest answer and
/// records a miss against every spool owner, which is indistinguishable from the
/// whole group failing at once. Blank rounds in a row are read as the view being
/// wrong, not the group.
///
/// It counts and decides, nothing more. What made a round blank is the caller's
/// knowledge, and the caller says so by which entry point it calls.
pub struct Tripwire {
    threshold: u64,
    inner: Mutex<TripwireState>,
}

struct TripwireState {
    // Counted per group. A node seated in several groups settles a round in each
    // of them, and one healthy group must not clear another group's run.
    blank_rounds: HashMap<GroupIndex, u64>,
    is_realigning: bool,
    has_tripped: bool,
    backoff: Backoff,
}

impl TripwireState {
    /// Suspends settlement and says how long to wait before re-reading state.
    ///
    /// The first trip is immediate; a repeat waits out a jittered backoff.
    fn suspend(&mut self) -> Duration {
        self.blank_rounds.clear();
        self.is_realigning = true;

        match self.has_tripped {
            false => {
                self.has_tripped = true;
                Duration::ZERO
            }
            true => self.backoff.next_delay().unwrap_or(REPEAT_BACKOFF.max_delay),
        }
    }
}

impl Tripwire {
    pub fn new(threshold: u64) -> Self {
        Self {
            threshold: threshold.max(1),
            inner: Mutex::new(TripwireState {
                blank_rounds: HashMap::new(),
                is_realigning: false,
                has_tripped: false,
                backoff: Backoff::new(REPEAT_BACKOFF),
            }),
        }
    }

    /// Whether settlement is suspended pending a realign
    pub fn is_realigning(&self) -> bool {
        self.lock().is_realigning
    }

    pub fn blank_rounds(&self, group: GroupIndex) -> u64 {
        self.lock().blank_rounds.get(&group).copied().unwrap_or_default()
    }

    /// Counts a round in which nothing this node weighed stood, and returns the
    /// delay to realign after once the run is long enough.
    pub fn record_blank_round(&self, group: GroupIndex) -> Option<Duration> {
        let mut inner = self.lock();

        let run = inner.blank_rounds.entry(group).or_default();
        *run += 1;
        let is_at_threshold = *run >= self.threshold;
        if inner.is_realigning || !is_at_threshold {
            return None;
        }

        Some(inner.suspend())
    }

    /// Clears a group's run after a round in which an answer stood.
    ///
    /// A node settling rounds successfully is not the node whose view is under
    /// suspicion, so the next trip from any source is urgent again.
    pub fn record_clean_round(&self, group: GroupIndex) {
        let mut inner = self.lock();
        inner.blank_rounds.remove(&group);
        inner.has_tripped = false;
        inner.backoff.reset();
    }

    /// Trips without a run of blank rounds behind it, for tests
    ///
    /// Returns nothing when a realign is already running, so a burst costs one
    /// read of the chain rather than one each.
    #[cfg(test)]
    pub fn trip(&self) -> Option<Duration> {
        let mut inner = self.lock();
        if inner.is_realigning {
            return None;
        }

        Some(inner.suspend())
    }

    /// The realign finished, whatever it found; settlement resumes
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

    // the run has to reach the threshold, and the first trip fires at once
    #[test]
    fn trips_at_threshold() {
        let tripwire = Tripwire::new(3);

        assert_eq!(tripwire.record_blank_round(ONE), None);
        assert_eq!(tripwire.record_blank_round(ONE), None);
        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
        assert!(tripwire.is_realigning());
    }

    // a node seated in two groups settles a round in each, and a run counted
    // across both would trip at half the threshold
    #[test]
    fn runs_are_per_group() {
        let tripwire = Tripwire::new(3);

        for _ in 0..2 {
            assert_eq!(tripwire.record_blank_round(ONE), None);
            assert_eq!(tripwire.record_blank_round(TWO), None);
        }

        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.blank_rounds(TWO), 2);
        assert!(!tripwire.is_realigning());
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
    }

    // and one group still certifying must not clear another group's run, or a
    // node seated in several groups never trips at all
    #[test]
    fn a_healthy_group_does_not_mask_a_blank_one() {
        let tripwire = Tripwire::new(3);

        for _ in 0..2 {
            tripwire.record_blank_round(ONE);
            tripwire.record_clean_round(TWO);
        }

        assert_eq!(tripwire.blank_rounds(ONE), 2);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
    }

    // one round where an answer stood clears that group's run
    #[test]
    fn clean_round_resets() {
        let tripwire = Tripwire::new(3);

        tripwire.record_blank_round(ONE);
        tripwire.record_blank_round(ONE);
        tripwire.record_clean_round(ONE);
        assert_eq!(tripwire.blank_rounds(ONE), 0);

        assert_eq!(tripwire.record_blank_round(ONE), None);
        assert_eq!(tripwire.record_blank_round(ONE), None);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
    }

    // nothing trips again while a realign is still running
    #[test]
    fn holds_while_realigning() {
        let tripwire = Tripwire::new(2);

        tripwire.record_blank_round(ONE);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));

        for _ in 0..5 {
            assert_eq!(tripwire.record_blank_round(ONE), None);
        }
        assert_eq!(tripwire.trip(), None);
        assert!(tripwire.is_realigning());
    }

    // a realign that did not fix it must not put the node back on the chain
    // every round, so the repeat waits
    #[test]
    fn repeat_backs_off() {
        let tripwire = Tripwire::new(2);

        tripwire.record_blank_round(ONE);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
        tripwire.settled();
        assert!(!tripwire.is_realigning());

        tripwire.record_blank_round(ONE);
        let delay = tripwire.record_blank_round(ONE).expect("second trip");
        assert!(delay > Duration::ZERO);
        assert!(delay <= REPEAT_BACKOFF.max_delay);
    }

    // a clean round between trips makes the next one urgent again
    #[test]
    fn clean_round_clears_the_backoff() {
        let tripwire = Tripwire::new(2);

        tripwire.record_blank_round(ONE);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
        tripwire.settled();

        tripwire.record_clean_round(ONE);
        tripwire.record_blank_round(ONE);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
    }

    // a zero threshold would realign on the first blank round of every epoch
    #[test]
    fn threshold_has_a_floor() {
        let tripwire = Tripwire::new(0);
        assert_eq!(tripwire.record_blank_round(ONE), Some(Duration::ZERO));
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

    // a trip that did not fix it escalates the same way a run of blank rounds
    // does, rather than firing a fresh scan the instant the last one released
    #[test]
    fn direct_trips_back_off() {
        let tripwire = Tripwire::new(8);

        assert_eq!(tripwire.trip(), Some(Duration::ZERO));
        tripwire.settled();

        let second = tripwire.trip().expect("second trip");
        assert!(second > Duration::ZERO);
        tripwire.settled();

        let third = tripwire.trip().expect("third trip");
        assert!(third > Duration::ZERO);
    }
}
