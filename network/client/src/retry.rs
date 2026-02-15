//! Retry configuration and execution helper.

use std::future::Future;
use std::time::Duration;

use crate::error::NodeError;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::fast()
    }
}

impl RetryConfig {
    /// No retries — fail immediately.
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Fast retry preset: 3 retries, 100ms base, 2s max.
    pub fn fast() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
        }
    }

    /// Resilient retry preset: 10 retries, 1s base, 30s max.
    pub fn resilient() -> Self {
        Self {
            max_retries: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }

    /// Compute the backoff delay for a given attempt (0-indexed).
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay = self.base_delay.saturating_mul(1u32.wrapping_shl(attempt));
        delay.min(self.max_delay)
    }
}

/// Execute an async operation with retry on transient failures.
pub async fn with_retry<T, F, Fut>(config: &RetryConfig, mut operation: F) -> Result<T, NodeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NodeError>>,
{
    let mut last_err = None;
    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if !e.is_retryable() || attempt == config.max_retries {
                    return Err(e);
                }
                let delay = config.backoff_delay(attempt);
                tracing::debug!(attempt, ?delay, "retrying after transient error: {e}");
                tokio::time::sleep(delay).await;
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delay() {
        let config = RetryConfig::fast();
        assert_eq!(config.backoff_delay(0), Duration::from_millis(100));
        assert_eq!(config.backoff_delay(1), Duration::from_millis(200));
        assert_eq!(config.backoff_delay(2), Duration::from_millis(400));
        assert_eq!(config.backoff_delay(3), Duration::from_millis(800));
        // Capped at max
        assert_eq!(config.backoff_delay(10), Duration::from_secs(2));
    }

    #[test]
    fn retry_presets() {
        let nr = RetryConfig::no_retry();
        assert_eq!(nr.max_retries, 0);

        let fast = RetryConfig::fast();
        assert_eq!(fast.max_retries, 3);
        assert_eq!(fast.base_delay, Duration::from_millis(100));
        assert_eq!(fast.max_delay, Duration::from_secs(2));

        let resilient = RetryConfig::resilient();
        assert_eq!(resilient.max_retries, 10);
        assert_eq!(resilient.base_delay, Duration::from_secs(1));
        assert_eq!(resilient.max_delay, Duration::from_secs(30));
    }
}
