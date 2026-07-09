use std::collections::HashSet;
use std::time::{Duration, Instant};

use rpc::RpcError;

/// Manages endpoint rotation with round-robin and tried-endpoint tracking
///
/// Keeps track of which endpoints have been tried and rotates through
/// untried endpoints in a round-robin fashion. An endpoint that fails is held
/// down for a cooldown, so the earliest healthy endpoint wins the next
/// operation and a brief outage does not move traffic off the primary for good.
#[derive(Debug)]
pub struct EndpointFailover {
    endpoints: Vec<String>,
    current: usize,
    tried: HashSet<usize>,
    max_attempts: u32,
    cooldown: Duration,
    held_down_until: Vec<Option<Instant>>,
}

impl EndpointFailover {
    /// Creates a new failover manager with the given endpoints
    ///
    /// # Arguments
    /// * `endpoints` - List of RPC endpoint URLs to rotate through
    /// * `max_attempts` - Maximum number of different endpoints to try
    /// * `cooldown` - How long a failed endpoint is skipped when picking a fresh one
    pub fn new(endpoints: Vec<String>, max_attempts: u32, cooldown: Duration) -> Self {
        let current = 0;
        let mut tried = HashSet::new();
        tried.insert(current); // Mark first endpoint as tried

        let held_down_until = vec![None; endpoints.len()];

        Self {
            endpoints,
            current,
            tried,
            max_attempts,
            cooldown,
            held_down_until,
        }
    }

    /// Get the current endpoint URL
    pub fn current_endpoint(&self) -> &str {
        &self.endpoints[self.current]
    }

    /// Get the index of the current endpoint
    #[cfg(any(test, feature = "metrics"))]
    pub fn current_index(&self) -> usize {
        self.current
    }

    /// True when this endpoint is not serving out a cooldown
    fn is_healthy(&self, index: usize, now: Instant) -> bool {
        self.held_down_until[index].is_none_or(|until| now >= until)
    }

    /// The endpoint a fresh operation should start on
    ///
    /// The earliest healthy endpoint, so the primary is preferred as soon as its
    /// cooldown lapses. When every endpoint is held down, stay where we are
    /// rather than reviving one early.
    fn preferred(&self, now: Instant) -> usize {
        (0..self.endpoints.len())
            .find(|index| self.is_healthy(*index, now))
            .unwrap_or(self.current)
    }

    /// Try to switch to the next untried endpoint
    ///
    /// Returns Ok(new_url) if a new endpoint is available,
    /// or Err if all endpoints have been exhausted.
    pub fn next_endpoint(&mut self) -> Result<&str, RpcError> {
        // Check if we've exhausted our attempts
        if self.tried.len() >= self.max_attempts as usize
            || self.tried.len() >= self.endpoints.len()
        {
            return Err(RpcError::AllEndpointsFailed {
                attempts: self.tried.len() as u32,
            });
        }

        // The endpoint we are leaving just failed us; hold it down so the next
        // operation does not walk straight back into it.
        self.held_down_until[self.current] = Some(Instant::now() + self.cooldown);

        // Round-robin to next untried endpoint
        let start = self.current;
        loop {
            self.current = (self.current + 1) % self.endpoints.len();

            // If we've looped back to start and haven't found an untried endpoint,
            // all endpoints have been tried
            if self.current == start && self.tried.len() > 0 {
                return Err(RpcError::AllEndpointsFailed {
                    attempts: self.tried.len() as u32,
                });
            }

            if !self.tried.contains(&self.current) {
                break;
            }
        }

        self.tried.insert(self.current);
        Ok(&self.endpoints[self.current])
    }

    /// Reset the failover state for a new operation
    ///
    /// Moves to the earliest healthy endpoint and clears the tried set. Returns
    /// true when the endpoint changed, so the caller can rebuild its client.
    pub fn reset(&mut self) -> bool {
        let preferred = self.preferred(Instant::now());
        let changed = preferred != self.current;

        self.current = preferred;
        self.tried.clear();
        self.tried.insert(self.current);
        changed
    }

    /// Get the number of endpoints that have been tried
    #[cfg(test)]
    pub fn tried_count(&self) -> usize {
        self.tried.len()
    }

    /// Get the total number of available endpoints
    #[cfg(test)]
    pub fn total_endpoints(&self) -> usize {
        self.endpoints.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Long enough that no test trips it by accident.
    const COOLDOWN: Duration = Duration::from_secs(60);

    fn failover(endpoints: Vec<String>, max_attempts: u32) -> EndpointFailover {
        EndpointFailover::new(endpoints, max_attempts, COOLDOWN)
    }

    #[test]
    fn test_initial_state() {
        let endpoints = vec!["http://endpoint1".to_string(), "http://endpoint2".to_string()];
        let failover = failover(endpoints.clone(), 3);

        assert_eq!(failover.current_endpoint(), "http://endpoint1");
        assert_eq!(failover.current_index(), 0);
        assert_eq!(failover.tried_count(), 1);
        assert_eq!(failover.total_endpoints(), 2);
    }

    #[test]
    fn test_round_robin() {
        let endpoints = vec![
            "http://endpoint1".to_string(),
            "http://endpoint2".to_string(),
            "http://endpoint3".to_string(),
        ];
        let mut failover = failover(endpoints, 3);

        // Should rotate to endpoint2
        let next = failover.next_endpoint().unwrap();
        assert_eq!(next, "http://endpoint2");
        assert_eq!(failover.tried_count(), 2);

        // Should rotate to endpoint3
        let next = failover.next_endpoint().unwrap();
        assert_eq!(next, "http://endpoint3");
        assert_eq!(failover.tried_count(), 3);

        // Should fail - all endpoints tried
        let result = failover.next_endpoint();
        assert!(result.is_err());
        match result {
            Err(RpcError::AllEndpointsFailed { attempts }) => assert_eq!(attempts, 3),
            _ => panic!("Expected AllEndpointsFailed error"),
        }
    }

    #[test]
    fn test_max_attempts_limit() {
        let endpoints = vec![
            "http://endpoint1".to_string(),
            "http://endpoint2".to_string(),
            "http://endpoint3".to_string(),
            "http://endpoint4".to_string(),
        ];
        let mut failover = failover(endpoints, 2); // Only allow 2 attempts

        // First switch should work
        assert!(failover.next_endpoint().is_ok());
        assert_eq!(failover.tried_count(), 2);

        // Second switch should fail due to max_attempts
        let result = failover.next_endpoint();
        assert!(result.is_err());
        match result {
            Err(RpcError::AllEndpointsFailed { attempts }) => assert_eq!(attempts, 2),
            _ => panic!("Expected AllEndpointsFailed error"),
        }
    }

    #[test]
    fn test_reset() {
        let endpoints = vec![
            "http://endpoint1".to_string(),
            "http://endpoint2".to_string(),
            "http://endpoint3".to_string(),
        ];
        let mut failover = failover(endpoints, 3);

        // Try a couple endpoints
        failover.next_endpoint().unwrap();
        failover.next_endpoint().unwrap();
        assert_eq!(failover.tried_count(), 3);

        // Reset should clear tried set but keep current endpoint
        let current = failover.current_index();
        failover.reset();
        assert_eq!(failover.tried_count(), 1);
        assert_eq!(failover.current_index(), current);
    }

    #[test]
    fn test_single_endpoint() {
        let endpoints = vec!["http://endpoint1".to_string()];
        let mut failover = failover(endpoints, 3);

        assert_eq!(failover.current_endpoint(), "http://endpoint1");

        // Should immediately fail when trying to switch
        let result = failover.next_endpoint();
        assert!(result.is_err());
    }

    // a failed primary stays skipped while its cooldown runs
    #[test]
    fn holds_down_failed_endpoint() {
        let endpoints = vec!["http://primary".to_string(), "http://fallback".to_string()];
        let mut failover = failover(endpoints, 3);

        failover.next_endpoint().expect("fail over");
        assert_eq!(failover.current_endpoint(), "http://fallback");

        // A fresh operation must not walk straight back into the dead primary.
        assert!(!failover.reset());
        assert_eq!(failover.current_endpoint(), "http://fallback");
    }

    // once the cooldown lapses the primary is preferred again
    #[test]
    fn returns_to_primary() {
        let endpoints = vec!["http://primary".to_string(), "http://fallback".to_string()];
        let mut failover =
            EndpointFailover::new(endpoints, 3, Duration::from_millis(20));

        failover.next_endpoint().expect("fail over");
        assert_eq!(failover.current_endpoint(), "http://fallback");

        std::thread::sleep(Duration::from_millis(40));

        assert!(failover.reset());
        assert_eq!(failover.current_endpoint(), "http://primary");
        assert_eq!(failover.tried_count(), 1);
    }

    // with every endpoint held down, stay put rather than reviving one early
    #[test]
    fn all_held_down_stays_put() {
        let endpoints = vec![
            "http://one".to_string(),
            "http://two".to_string(),
            "http://three".to_string(),
        ];
        let mut failover = failover(endpoints, 3);

        failover.next_endpoint().expect("second");
        failover.next_endpoint().expect("third");

        // 0 and 1 are held down; 2 is current and never failed, so it wins.
        assert!(!failover.reset());
        assert_eq!(failover.current_endpoint(), "http://three");
    }

    #[test]
    fn test_wraps_around() {
        let endpoints = vec![
            "http://endpoint1".to_string(),
            "http://endpoint2".to_string(),
            "http://endpoint3".to_string(),
        ];
        let mut failover = failover(endpoints, 10); // High max attempts

        // Start at 0, should go 1, 2, then wrap to 0, but 0 is already tried
        assert_eq!(failover.current_index(), 0);

        failover.next_endpoint().unwrap();
        assert_eq!(failover.current_index(), 1);

        failover.next_endpoint().unwrap();
        assert_eq!(failover.current_index(), 2);

        // All endpoints tried now
        let result = failover.next_endpoint();
        assert!(result.is_err());
    }
}
