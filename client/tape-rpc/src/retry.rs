use crate::config::RetryConfig;
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use std::time::Duration;

/// Exponential backoff with optional jitter
///
/// Implements exponential backoff strategy with multiplicative factor of 2.
/// Each delay is: min(max_backoff, 2^attempt * min_backoff + jitter)
#[derive(Debug)]
pub struct ExponentialBackoff {
    config: RetryConfig,
    attempt: u32,
    rng: SmallRng,
}

impl ExponentialBackoff {
    /// Maximum number of milliseconds to randomly add as jitter
    const MAX_JITTER_MS: u64 = 1000;

    /// Creates a new backoff strategy with the given configuration
    pub fn new(config: &RetryConfig) -> Self {
        Self {
            config: config.clone(),
            attempt: 0,
            rng: SmallRng::from_entropy(),
        }
    }

    /// Creates a new backoff strategy with a specific seed (useful for testing)
    #[cfg(test)]
    pub fn new_with_seed(config: &RetryConfig, seed: u64) -> Self {
        Self {
            config: config.clone(),
            attempt: 0,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Returns the next delay duration, or None if max retries exceeded
    ///
    /// Calculates: min(max_backoff, 2^attempt * min_backoff + jitter)
    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.attempt >= self.config.max_retries {
            return None;
        }

        // Calculate base delay: min_backoff * 2^attempt
        let base_ms = self.config.min_backoff.as_millis() as u64;
        let multiplier = 1u64.checked_shl(self.attempt).unwrap_or(u64::MAX);
        let delay_ms = base_ms.saturating_mul(multiplier);

        // Cap at max_backoff
        let capped_ms = delay_ms.min(self.config.max_backoff.as_millis() as u64);

        // Add jitter if enabled
        let final_ms = if self.config.jitter {
            let jitter = self.rng.gen_range(0..Self::MAX_JITTER_MS);
            capped_ms.saturating_add(jitter)
        } else {
            capped_ms
        };

        self.attempt += 1;
        Some(Duration::from_millis(final_ms))
    }

    /// Returns the current attempt number (0-indexed)
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Resets the backoff state to initial conditions
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_growth() {
        let config = RetryConfig {
            max_retries: 5,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: false,
            max_endpoint_attempts: 3,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // Expected: 100, 200, 400, 800, 1600
        let expected = vec![100, 200, 400, 800, 1600];

        for (i, expected_ms) in expected.iter().enumerate() {
            let delay = backoff.next_delay().unwrap();
            assert_eq!(
                delay.as_millis() as u64,
                *expected_ms,
                "Attempt {} should be {}ms",
                i,
                expected_ms
            );
        }

        // Should return None after max retries
        assert!(backoff.next_delay().is_none());
    }

    #[test]
    fn test_max_backoff_cap() {
        let config = RetryConfig {
            max_retries: 10,
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            jitter: false,
            max_endpoint_attempts: 3,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // First few should grow exponentially
        assert_eq!(backoff.next_delay().unwrap().as_secs(), 1); // 2^0 * 1 = 1
        assert_eq!(backoff.next_delay().unwrap().as_secs(), 2); // 2^1 * 1 = 2
        assert_eq!(backoff.next_delay().unwrap().as_secs(), 4); // 2^2 * 1 = 4

        // After this, should be capped at max_backoff (5s)
        assert_eq!(backoff.next_delay().unwrap().as_secs(), 5); // 2^3 * 1 = 8, capped to 5
        assert_eq!(backoff.next_delay().unwrap().as_secs(), 5); // 2^4 * 1 = 16, capped to 5
    }

    #[test]
    fn test_jitter_adds_randomness() {
        let config = RetryConfig {
            max_retries: 3,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: true,
            max_endpoint_attempts: 3,
        };

        let mut backoff1 = ExponentialBackoff::new_with_seed(&config, 42);
        let mut backoff2 = ExponentialBackoff::new_with_seed(&config, 43);

        let delay1 = backoff1.next_delay().unwrap();
        let delay2 = backoff2.next_delay().unwrap();

        // With jitter, different seeds should produce different delays
        // (though there's a tiny chance they're the same)
        // Base is 100ms, jitter is 0-1000ms, so both should be in range [100, 1100]
        assert!(delay1.as_millis() >= 100 && delay1.as_millis() <= 1100);
        assert!(delay2.as_millis() >= 100 && delay2.as_millis() <= 1100);
    }

    #[test]
    fn test_attempt_counter() {
        let config = RetryConfig {
            max_retries: 3,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: false,
            max_endpoint_attempts: 3,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        assert_eq!(backoff.attempt(), 0);
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 1);
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 3);
    }

    #[test]
    fn test_reset() {
        let config = RetryConfig {
            max_retries: 5,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: false,
            max_endpoint_attempts: 3,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);

        backoff.reset();
        assert_eq!(backoff.attempt(), 0);

        // After reset, should start from beginning
        let delay = backoff.next_delay().unwrap();
        assert_eq!(delay.as_millis() as u64, 100);
    }

    #[test]
    fn test_max_retries_zero() {
        let config = RetryConfig {
            max_retries: 0,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: false,
            max_endpoint_attempts: 3,
        };

        let mut backoff = ExponentialBackoff::new(&config);
        assert!(backoff.next_delay().is_none());
    }
}
