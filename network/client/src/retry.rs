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

    /// Upload retry preset: 8 retries, 2s base, 30s max.
    ///
    /// Tuned for slice uploads where nodes may not have ingested the track
    /// yet. On mainnet Solana, block confirmation takes 15-30s, so retries
    /// must span that window: 2s, 4s, 8s, 16s, 30s, 30s, 30s, 30s (~150s total).
    pub fn upload() -> Self {
        Self {
            max_retries: 8,
            base_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(30),
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
///
/// Only retries errors where [`NodeError::is_retryable`] returns true.
/// Use [`with_retry_all`] when all errors should be retried (e.g. timing races).
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

/// Execute an async operation with unconditional retry and exponential backoff.
///
/// Unlike [`with_retry`], this retries on ALL errors regardless of type.
/// Useful when errors are expected to be transient due to timing (e.g. a node
/// returning 404 because it hasn't ingested a track yet).
pub async fn with_retry_all<T, E, F, Fut>(config: &RetryConfig, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_err = None;
    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if attempt == config.max_retries {
                    return Err(e);
                }
                let delay = config.backoff_delay(attempt);
                tracing::debug!(attempt, ?delay, "retrying after error: {e}");
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

        let upload = RetryConfig::upload();
        assert_eq!(upload.max_retries, 8);
        assert_eq!(upload.base_delay, Duration::from_secs(2));
        assert_eq!(upload.max_delay, Duration::from_secs(30));

        let resilient = RetryConfig::resilient();
        assert_eq!(resilient.max_retries, 10);
        assert_eq!(resilient.base_delay, Duration::from_secs(1));
        assert_eq!(resilient.max_delay, Duration::from_secs(30));
    }
}
