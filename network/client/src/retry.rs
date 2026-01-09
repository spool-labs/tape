//! Retry utilities for node client operations.
//!
//! Provides configurable retry with exponential backoff for transient failures.

use std::future::Future;
use std::time::Duration;

use crate::error::NodeError;

/// Configuration for retry behavior.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries, just the initial attempt).
    pub max_retries: u32,
    /// Base delay for exponential backoff (doubles each retry).
    pub base_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl RetryConfig {
    /// Create a config with no retries.
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a config optimized for fast networks.
    pub fn fast() -> Self {
        Self {
            max_retries: 1,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(500),
        }
    }

    /// Create a config optimized for unreliable networks.
    pub fn resilient() -> Self {
        Self {
            max_retries: 5,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }

    /// Calculate backoff delay for a given attempt number.
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay = self.base_delay * 2u32.saturating_pow(attempt);
        std::cmp::min(delay, self.max_delay)
    }
}

/// Execute an async operation with retry on transient failures.
///
/// # Arguments
/// * `config` - Retry configuration
/// * `operation` - Async function that returns `Result<T, NodeError>`
///
/// # Returns
/// The result of the operation, or the last error if all retries failed.
///
/// # Example
/// ```ignore
/// use tape_node_client::retry::{RetryConfig, with_retry};
///
/// let result = with_retry(&RetryConfig::default(), || async {
///     client.get_signature(track_id).await
/// }).await;
/// ```
pub async fn with_retry<T, F, Fut>(config: &RetryConfig, mut operation: F) -> Result<T, NodeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NodeError>>,
{
    let mut last_error = NodeError::Timeout; // Placeholder, will be overwritten

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = e;

                // Don't retry non-transient errors
                if !last_error.is_retryable() {
                    return Err(last_error);
                }

                // Don't sleep after the last attempt
                if attempt < config.max_retries {
                    let delay = config.backoff_delay(attempt);
                    log::debug!(
                        "Retrying after transient error: attempt={}/{}, delay={}ms, error={}",
                        attempt + 1,
                        config.max_retries,
                        delay.as_millis(),
                        last_error
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(2));
    }

    #[test]
    fn test_retry_config_no_retry() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_backoff_calculation() {
        let config = RetryConfig {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            ..Default::default()
        };

        // Exponential: 100ms, 200ms, 400ms, 800ms, 1600ms, capped at 2000ms
        assert_eq!(config.backoff_delay(0), Duration::from_millis(100));
        assert_eq!(config.backoff_delay(1), Duration::from_millis(200));
        assert_eq!(config.backoff_delay(2), Duration::from_millis(400));
        assert_eq!(config.backoff_delay(3), Duration::from_millis(800));
        assert_eq!(config.backoff_delay(4), Duration::from_millis(1600));
        assert_eq!(config.backoff_delay(5), Duration::from_secs(2)); // capped
        assert_eq!(config.backoff_delay(10), Duration::from_secs(2)); // still capped
    }

    #[tokio::test]
    async fn test_with_retry_immediate_success() {
        let config = RetryConfig::default();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, NodeError> = with_retry(&config, || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_with_retry_non_retryable_error() {
        let config = RetryConfig::default();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, NodeError> = with_retry(&config, || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(NodeError::NotFound) // Not retryable
            }
        })
        .await;

        assert!(matches!(result, Err(NodeError::NotFound)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // No retries
    }

    #[tokio::test]
    async fn test_with_retry_eventual_success() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(1), // Fast for testing
            max_delay: Duration::from_millis(10),
        };
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, NodeError> = with_retry(&config, || {
            let attempts = attempts_clone.clone();
            async move {
                let count = attempts.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    Err(NodeError::Timeout) // Retryable
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn test_with_retry_exhausted() {
        let config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
        };
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result: Result<i32, NodeError> = with_retry(&config, || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(NodeError::Timeout)
            }
        })
        .await;

        assert!(matches!(result, Err(NodeError::Timeout)));
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }
}
