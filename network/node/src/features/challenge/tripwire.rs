use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

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
    blank_rounds: u64,
    is_realigning: bool,
    has_tripped: bool,
    backoff: Backoff,
}

impl Tripwire {
    pub fn new(threshold: u64) -> Self {
        Self {
            threshold: threshold.max(1),
            inner: Mutex::new(TripwireState {
                blank_rounds: 0,
                is_realigning: false,
                has_tripped: false,
                backoff: Backoff::new(REPEAT_BACKOFF),
            }),
        }
    }

    /// Whether judging and attesting are suspended pending a realign
    pub fn is_realigning(&self) -> bool {
        self.lock().is_realigning
    }

    pub fn blank_rounds(&self) -> u64 {
        self.lock().blank_rounds
    }

    /// Feeds one settled round, returning the delay to realign after if it trips
    ///
    /// The first trip is immediate; a repeat waits out a jittered backoff. A
    /// clean round clears both the run and the backoff.
    pub fn record_round(&self, judgement: Judgement) -> Option<Duration> {
        let mut inner = self.lock();

        if !judgement.is_blank() {
            inner.blank_rounds = 0;
            inner.has_tripped = false;
            inner.backoff.reset();
            return None;
        }

        inner.blank_rounds += 1;
        if inner.is_realigning || inner.blank_rounds < self.threshold {
            return None;
        }

        Some(arm(&mut inner))
    }

    /// Trips on evidence from outside the round path
    ///
    /// Returns nothing when a realign is already running, so a burst of reports
    /// costs one read of the chain rather than one each.
    pub fn trip(&self) -> Option<Duration> {
        let mut inner = self.lock();
        if inner.is_realigning {
            return None;
        }

        Some(arm(&mut inner))
    }

    /// The realign finished, whatever it found; judging resumes
    pub fn settled(&self) {
        let mut inner = self.lock();
        inner.is_realigning = false;
        inner.blank_rounds = 0;
    }

    fn lock(&self) -> MutexGuard<'_, TripwireState> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Suspends judging and returns how long to wait before re-reading state.
fn arm(inner: &mut TripwireState) -> Duration {
    inner.blank_rounds = 0;
    inner.is_realigning = true;
    match inner.has_tripped {
        false => {
            inner.has_tripped = true;
            Duration::ZERO
        }
        true => inner.backoff.next_delay().unwrap_or(REPEAT_BACKOFF.max_delay),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(tripwire.record_round(blank()), None);
        assert_eq!(tripwire.record_round(blank()), None);
        assert_eq!(tripwire.blank_rounds(), 2);
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
        assert!(tripwire.is_realigning());
    }

    // one round where a peer stood clears the run, so a bad patch does not
    // accumulate across an epoch
    #[test]
    fn clean_round_resets() {
        let tripwire = Tripwire::new(3);

        tripwire.record_round(blank());
        tripwire.record_round(blank());
        assert_eq!(tripwire.record_round(clean()), None);
        assert_eq!(tripwire.blank_rounds(), 0);

        assert_eq!(tripwire.record_round(blank()), None);
        assert_eq!(tripwire.record_round(blank()), None);
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
    }

    // nothing trips again while a realign is still running
    #[test]
    fn holds_while_realigning() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(blank());
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));

        for _ in 0..5 {
            assert_eq!(tripwire.record_round(blank()), None);
        }
        assert!(tripwire.is_realigning());
    }

    // a realign that did not fix it must not put the node back on the chain
    // every round, so the repeat waits
    #[test]
    fn repeat_backs_off() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(blank());
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
        tripwire.settled();
        assert!(!tripwire.is_realigning());

        tripwire.record_round(blank());
        let delay = tripwire.record_round(blank()).expect("second trip");
        assert!(delay > Duration::ZERO);
        assert!(delay <= REPEAT_BACKOFF.max_delay);
    }

    // a clean round between trips makes the next one urgent again
    #[test]
    fn clean_round_clears_the_backoff() {
        let tripwire = Tripwire::new(2);

        tripwire.record_round(blank());
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
        tripwire.settled();

        assert_eq!(tripwire.record_round(clean()), None);
        tripwire.record_round(blank());
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
    }

    // a zero threshold would realign on the first blank round of every epoch
    #[test]
    fn threshold_has_a_floor() {
        let tripwire = Tripwire::new(0);
        assert_eq!(tripwire.record_round(blank()), Some(Duration::ZERO));
    }

    // evidence off the round path arms the same suspension, and a burst of it
    // costs one read rather than one each
    #[test]
    fn trips_from_outside() {
        let tripwire = Tripwire::new(8);

        assert_eq!(tripwire.trip(), Some(Duration::ZERO));
        assert_eq!(tripwire.trip(), None);
        assert!(tripwire.is_realigning());

        tripwire.settled();
        assert!(tripwire.trip().is_some_and(|delay| delay > Duration::ZERO));
    }
}
