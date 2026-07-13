//! Picks which RPC endpoint each operation runs on

use std::collections::HashSet;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use rpc::RpcError;

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
/// one operation, rotation on error walks the untried endpoints in ring order
/// regardless of strategy.
#[derive(Debug)]
pub struct EndpointSelector {
    endpoints: Vec<String>,
    strategy: EndpointStrategy,
    current: usize,
    max_attempts: u32,
    cooldown: Duration,
    held_down_until: Vec<Option<Instant>>,
}

/// One operation's endpoint state: where it runs and what it already tried
///
/// Owned by the operation, so concurrent operations rotate independently.
#[derive(Debug)]
pub struct EndpointCursor {
    index: usize,
    tried: HashSet<usize>,
}

impl EndpointCursor {
    /// The endpoint index this operation currently runs on
    pub fn index(&self) -> usize {
        self.index
    }
}

impl EndpointSelector {
    /// Creates a new selector over the given endpoints
    ///
    /// One operation tries at most max_attempts endpoints; a failed endpoint
    /// is skipped for the cooldown when picking a fresh start.
    pub fn new(
        endpoints: Vec<String>,
        strategy: EndpointStrategy,
        max_attempts: u32,
        cooldown: Duration,
    ) -> Self {
        let held_down_until = vec![None; endpoints.len()];

        Self {
            endpoints,
            strategy,
            current: 0,
            max_attempts,
            cooldown,
            held_down_until,
        }
    }

    /// The endpoint URL at this index
    #[cfg(feature = "metrics")]
    pub fn endpoint(&self, index: usize) -> &str {
        &self.endpoints[index]
    }

    /// True when this endpoint is not serving out a cooldown
    fn is_healthy(&self, index: usize, now: Instant) -> bool {
        self.held_down_until[index].is_none_or(|until| now >= until)
    }

    /// The earliest healthy endpoint, if any
    fn earliest_healthy(&self, now: Instant) -> Option<usize> {
        for index in 0..self.endpoints.len() {
            if self.is_healthy(index, now) {
                return Some(index);
            }
        }

        None
    }

    /// The next healthy endpoint after the start index in ring order, if any
    fn next_healthy(&self, start: usize, now: Instant) -> Option<usize> {
        for step in 1..=self.endpoints.len() {
            let index = (start + step) % self.endpoints.len();
            if self.is_healthy(index, now) {
                return Some(index);
            }
        }

        None
    }

    /// The next endpoint after the cursor that the operation has not tried
    fn next_untried(&self, cursor: &EndpointCursor) -> Option<usize> {
        for step in 1..self.endpoints.len() {
            let index = (cursor.index + step) % self.endpoints.len();
            if !cursor.tried.contains(&index) {
                return Some(index);
            }
        }

        None
    }

    /// Pick the endpoint a fresh operation starts on
    ///
    /// Returns the operation cursor and whether the pick moved off the
    /// previous endpoint. When every endpoint is held down, stay where we are
    /// rather than reviving one early.
    pub fn start_operation(&mut self) -> (EndpointCursor, bool) {
        let now = Instant::now();

        let picked = match self.strategy {
            EndpointStrategy::PreferPrimary => self.earliest_healthy(now),
            EndpointStrategy::FailoverSticky => Some(self.current),
            EndpointStrategy::RoundRobin => self.next_healthy(self.current, now),
        };
        let picked = picked.unwrap_or(self.current);

        let is_moved = picked != self.current;
        self.current = picked;

        let mut tried = HashSet::new();
        tried.insert(picked);

        (EndpointCursor { index: picked, tried }, is_moved)
    }

    /// Rotate a failing operation to the next untried endpoint, holding the
    /// one it leaves down for the cooldown
    ///
    /// Returns Err when the operation has exhausted its endpoint attempts;
    /// the caller keeps retrying where it is via backoff.
    pub fn fail_over(&mut self, cursor: &mut EndpointCursor) -> Result<usize, RpcError> {
        // The endpoint we are leaving just failed us; hold it down so fresh
        // operations do not walk straight back into it.
        self.held_down_until[cursor.index] = Some(Instant::now() + self.cooldown);

        let has_budget = cursor.tried.len() < self.max_attempts as usize
            && cursor.tried.len() < self.endpoints.len();

        let next = if has_budget { self.next_untried(cursor) } else { None };
        let Some(next) = next else {
            return Err(RpcError::AllEndpointsFailed {
                attempts: cursor.tried.len() as u32,
            });
        };

        cursor.index = next;
        cursor.tried.insert(next);
        self.current = next;

        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Long enough that no test trips it by accident.
    const COOLDOWN: Duration = Duration::from_secs(60);

    fn urls(count: usize) -> Vec<String> {
        let mut out = Vec::new();
        for index in 0..count {
            out.push(format!("http://endpoint{index}"));
        }
        out
    }

    fn selector(strategy: EndpointStrategy, count: usize) -> EndpointSelector {
        EndpointSelector::new(urls(count), strategy, count as u32, COOLDOWN)
    }

    // prefer-primary and sticky both start a fresh selector on the first endpoint
    #[test]
    fn first_endpoint() {
        for strategy in [
            EndpointStrategy::PreferPrimary,
            EndpointStrategy::FailoverSticky,
        ] {
            let (cursor, is_moved) = selector(strategy, 3).start_operation();

            assert_eq!(cursor.index(), 0);
            assert!(!is_moved);
        }
    }

    // within one operation, rotation walks untried endpoints in ring order
    #[test]
    fn ring_rotation() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 3);
        let (mut cursor, _) = selector.start_operation();

        assert_eq!(selector.fail_over(&mut cursor).expect("second"), 1);
        assert_eq!(selector.fail_over(&mut cursor).expect("third"), 2);
        assert!(selector.fail_over(&mut cursor).is_err());
    }

    // an operation stops rotating once it hits its attempt budget
    #[test]
    fn attempt_budget() {
        let mut selector =
            EndpointSelector::new(urls(3), EndpointStrategy::PreferPrimary, 2, COOLDOWN);
        let (mut cursor, _) = selector.start_operation();

        assert_eq!(selector.fail_over(&mut cursor).expect("second"), 1);
        assert!(selector.fail_over(&mut cursor).is_err());
    }

    // prefer-primary picks the primary back up once its cooldown lapses
    #[test]
    fn primary_returns() {
        let mut selector =
            EndpointSelector::new(urls(2), EndpointStrategy::PreferPrimary, 2, Duration::ZERO);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor).expect("fallback");

        let (cursor, is_moved) = selector.start_operation();

        assert_eq!(cursor.index(), 0);
        assert!(is_moved);
    }

    // prefer-primary stays on the fallback while the primary is held down
    #[test]
    fn primary_held_down() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 2);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor).expect("fallback");

        let (cursor, is_moved) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_moved);
    }

    // failover-sticky never leaves an endpoint that has not failed
    #[test]
    fn sticky_stays() {
        let mut selector =
            EndpointSelector::new(urls(2), EndpointStrategy::FailoverSticky, 2, Duration::ZERO);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor).expect("fallback");

        // Even with the primary healthy again, sticky stays put.
        let (cursor, is_moved) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_moved);
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
        selector.fail_over(&mut cursor).expect("rotate");

        assert_eq!(selector.start_operation().0.index(), 0);
        assert_eq!(selector.start_operation().0.index(), 2);
        assert_eq!(selector.start_operation().0.index(), 0);
    }

    // when every endpoint is held down, stay put rather than reviving one early
    #[test]
    fn all_held_down() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 2);
        let (mut cursor, _) = selector.start_operation();
        selector.fail_over(&mut cursor).expect("fallback");
        assert!(selector.fail_over(&mut cursor).is_err());

        let (cursor, is_moved) = selector.start_operation();

        assert_eq!(cursor.index(), 1);
        assert!(!is_moved);
    }

    // a single endpoint has nowhere to rotate
    #[test]
    fn single_endpoint() {
        let mut selector = selector(EndpointStrategy::PreferPrimary, 1);
        let (mut cursor, _) = selector.start_operation();

        assert!(selector.fail_over(&mut cursor).is_err());
        assert_eq!(selector.start_operation().0.index(), 0);
    }
}
