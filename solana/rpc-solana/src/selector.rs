//! Picks which RPC endpoint each operation runs on

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// How a fresh operation picks the endpoint it starts on
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointStrategy {
    /// Start on the earliest healthy endpoint, so the primary wins traffic
    /// back as soon as its cooldown lapses
    #[default]
    PreferPrimary,

    /// Stay on the current endpoint until it actually fails
    FailoverSticky,

    /// Start on the next healthy endpoint after the previous start, wrapping
    RoundRobin,
}

/// Picks the endpoint each operation runs on
///
/// An endpoint that fails is held down for a cooldown so fresh operations
/// avoid it. The strategy only decides where a fresh operation starts; within
/// one operation, rotation on error walks the endpoints in ring order
/// regardless of strategy.
#[derive(Debug)]
pub struct EndpointSelector {
    endpoint_count: usize,
    strategy: EndpointStrategy,
    current: usize,
    max_attempts: u32,
    cooldown: Duration,
    held_down_until: Vec<Option<Instant>>,
}

/// One operation's endpoint state: where it runs and how many endpoints it
/// has tried
///
/// Rotation is strictly ring-order from the starting endpoint, so the set of
/// tried endpoints is always the contiguous run behind the index and a count
/// captures it. Owned by the operation, so concurrent operations rotate
/// independently.
#[derive(Clone, Copy, Debug)]
pub struct EndpointCursor {
    index: usize,
    attempts: usize,
}

impl EndpointCursor {
    /// The endpoint index this operation currently runs on
    pub fn index(&self) -> usize {
        self.index
    }
}

impl EndpointSelector {
    /// Creates a new selector over this many endpoints
    ///
    /// One operation tries at most max_attempts endpoints; a failed endpoint
    /// is skipped for the cooldown when picking a fresh start.
    pub fn new(
        endpoint_count: usize,
        strategy: EndpointStrategy,
        max_attempts: u32,
        cooldown: Duration,
    ) -> Self {
        Self {
            endpoint_count,
            strategy,
            current: 0,
            max_attempts,
            cooldown,
            held_down_until: vec![None; endpoint_count],
        }
    }

    /// True when this endpoint is not serving out a cooldown
    fn is_healthy(&self, index: usize, now: Instant) -> bool {
        self.held_down_until[index].is_none_or(|until| now >= until)
    }

    /// The earliest healthy endpoint, if any
    fn earliest_healthy(&self, now: Instant) -> Option<usize> {
        for index in 0..self.endpoint_count {
            if self.is_healthy(index, now) {
                return Some(index);
            }
        }

        None
    }

    /// The next healthy endpoint after the start index in ring order, if any
    fn next_healthy(&self, start: usize, now: Instant) -> Option<usize> {
        for step in 1..=self.endpoint_count {
            let index = (start + step) % self.endpoint_count;
            if self.is_healthy(index, now) {
                return Some(index);
            }
        }

        None
    }

    /// Pick the endpoint a fresh operation starts on
    ///
    /// Returns the operation cursor and whether a prefer-primary pick moved
    /// off the previous endpoint, which is the caller's cue to log the
    /// restored primary. When every endpoint is held down, stay where we are
    /// rather than reviving one early.
    pub fn start_operation(&mut self) -> (EndpointCursor, bool) {
        let now = Instant::now();

        let picked = match self.strategy {
            EndpointStrategy::PreferPrimary => self.earliest_healthy(now),
            EndpointStrategy::FailoverSticky => Some(self.current),
            EndpointStrategy::RoundRobin => self.next_healthy(self.current, now),
        };
        let picked = picked.unwrap_or(self.current);

        let is_restored = picked != self.current
            && matches!(self.strategy, EndpointStrategy::PreferPrimary);
        self.current = picked;

        (EndpointCursor { index: picked, attempts: 1 }, is_restored)
    }

    /// Rotate a failing operation to the next endpoint, holding the one it
    /// leaves down for the cooldown
    ///
    /// Returns false when the operation has exhausted its endpoint attempts;
    /// the caller keeps retrying where it is via backoff.
    pub fn fail_over(&mut self, cursor: &mut EndpointCursor) -> bool {
        // The endpoint we are leaving just failed us; hold it down so fresh
        // operations do not walk straight back into it.
        self.held_down_until[cursor.index] = Some(Instant::now() + self.cooldown);

        if cursor.attempts >= self.max_attempts as usize
            || cursor.attempts >= self.endpoint_count
        {
            return false;
        }

        cursor.index = (cursor.index + 1) % self.endpoint_count;
        cursor.attempts += 1;

        // Following the operation keeps sticky on the survivor and hands
        // round-robin a fresh rotation origin.
        self.current = cursor.index;

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Long enough that no test trips it by accident.
    const COOLDOWN: Duration = Duration::from_secs(60);

    fn selector(strategy: EndpointStrategy, count: usize) -> EndpointSelector {
        EndpointSelector::new(count, strategy, count as u32, COOLDOWN)
    }

    // prefer-primary and sticky both start a fresh selector on the first endpoint
    #[test]
    fn first_endpoint() {
        for strategy in [
            EndpointStrategy::PreferPrimary,
            EndpointStrategy::FailoverSticky,
        ] {
            let (cursor, is_restored) = selector(strategy, 3).start_operation();

            assert_eq!(cursor.index(), 0);
            assert!(!is_restored);
        }
    }

    // within one operation, rotation walks endpoints in ring order
    #[test]
    fn ring_rotation() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 3);
        let (mut cursor, _) = selector.start_operation();

        assert!(selector.fail_over(&mut cursor));
        assert_eq!(cursor.index(), 1);
        assert!(selector.fail_over(&mut cursor));
        assert_eq!(cursor.index(), 2);
        assert!(!selector.fail_over(&mut cursor));
    }

    // an operation stops rotating once it hits its attempt budget
    #[test]
    fn attempt_budget() {
        let mut selector =
            EndpointSelector::new(3, EndpointStrategy::PreferPrimary, 2, COOLDOWN);
        let (mut cursor, _) = selector.start_operation();

        assert!(selector.fail_over(&mut cursor));
        assert_eq!(cursor.index(), 1);
        assert!(!selector.fail_over(&mut cursor));
    }

    // prefer-primary picks the primary back up once its cooldown lapses
    #[test]
    fn primary_returns() {
        let mut selector =
            EndpointSelector::new(2, EndpointStrategy::PreferPrimary, 2, Duration::ZERO);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor);

        let (cursor, is_restored) = selector.start_operation();

        assert_eq!(cursor.index(), 0);
        assert!(is_restored);
    }

    // prefer-primary stays on the fallback while the primary is held down
    #[test]
    fn primary_held_down() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 2);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor);

        let (cursor, is_restored) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_restored);
    }

    // failover-sticky never leaves an endpoint that has not failed
    #[test]
    fn sticky_stays() {
        let mut selector =
            EndpointSelector::new(2, EndpointStrategy::FailoverSticky, 2, Duration::ZERO);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor);

        // Even with the primary healthy again, sticky stays put.
        let (cursor, is_restored) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_restored);
    }

    // round-robin rotates fresh operations across healthy endpoints
    #[test]
    fn round_robin_rotates() {
        let mut selector = selector(EndpointStrategy::RoundRobin, 3);

        assert_eq!(selector.start_operation().0.index(), 1);
        assert_eq!(selector.start_operation().0.index(), 2);
        assert_eq!(selector.start_operation().0.index(), 0);
    }

    // round-robin skips a held-down endpoint until its cooldown lapses
    #[test]
    fn round_robin_skips() {
        let mut selector = selector(EndpointStrategy::RoundRobin, 3);
        let (mut cursor, _) = selector.start_operation();
        assert_eq!(cursor.index(), 1);

        // Fail endpoint 1; rotation moves the operation to endpoint 2.
        selector.fail_over(&mut cursor);
        assert_eq!(cursor.index(), 2);

        assert_eq!(selector.start_operation().0.index(), 0);
        assert_eq!(selector.start_operation().0.index(), 2);
        assert_eq!(selector.start_operation().0.index(), 0);
    }

    // when every endpoint is held down, stay put rather than reviving one early
    #[test]
    fn all_held_down() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 2);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor);
        assert!(!selector.fail_over(&mut cursor));

        let (cursor, is_restored) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_restored);
    }

    // a single endpoint has nowhere to rotate
    #[test]
    fn single_endpoint() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 1);
        let (mut cursor, _) = selector.start_operation();

        assert!(!selector.fail_over(&mut cursor));
        assert_eq!(selector.start_operation().0.index(), 0);
    }
}
