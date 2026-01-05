use std::collections::HashSet;
use rpc::RpcError;

/// Manages endpoint rotation with round-robin and tried-endpoint tracking
///
/// Keeps track of which endpoints have been tried and rotates through
/// untried endpoints in a round-robin fashion.
#[derive(Debug)]
pub struct EndpointFailover {
    endpoints: Vec<String>,
    current: usize,
    tried: HashSet<usize>,
    max_attempts: u32,
}

impl EndpointFailover {
    /// Creates a new failover manager with the given endpoints
    ///
    /// # Arguments
    /// * `endpoints` - List of RPC endpoint URLs to rotate through
    /// * `max_attempts` - Maximum number of different endpoints to try
    pub fn new(endpoints: Vec<String>, max_attempts: u32) -> Self {
        let current = 0;
        let mut tried = HashSet::new();
        tried.insert(current); // Mark first endpoint as tried

        Self {
            endpoints,
            current,
            tried,
            max_attempts,
        }
    }

    /// Get the current endpoint URL
    pub fn current_endpoint(&self) -> &str {
        &self.endpoints[self.current]
    }

    /// Get the index of the current endpoint
    pub fn current_index(&self) -> usize {
        self.current
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
    /// Clears the tried set and marks the current endpoint as tried.
    pub fn reset(&mut self) {
        self.tried.clear();
        self.tried.insert(self.current);
    }

    /// Get the number of endpoints that have been tried
    pub fn tried_count(&self) -> usize {
        self.tried.len()
    }

    /// Get the total number of available endpoints
    pub fn total_endpoints(&self) -> usize {
        self.endpoints.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let endpoints = vec!["http://endpoint1".to_string(), "http://endpoint2".to_string()];
        let failover = EndpointFailover::new(endpoints.clone(), 3);

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
        let mut failover = EndpointFailover::new(endpoints, 3);

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
        let mut failover = EndpointFailover::new(endpoints, 2); // Only allow 2 attempts

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
        let mut failover = EndpointFailover::new(endpoints, 3);

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
        let mut failover = EndpointFailover::new(endpoints, 3);

        assert_eq!(failover.current_endpoint(), "http://endpoint1");

        // Should immediately fail when trying to switch
        let result = failover.next_endpoint();
        assert!(result.is_err());
    }

    #[test]
    fn test_wraps_around() {
        let endpoints = vec![
            "http://endpoint1".to_string(),
            "http://endpoint2".to_string(),
            "http://endpoint3".to_string(),
        ];
        let mut failover = EndpointFailover::new(endpoints, 10); // High max attempts

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
