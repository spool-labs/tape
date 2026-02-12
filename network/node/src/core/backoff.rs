//! Shared retry infrastructure with exponential backoff.
//!
//! All retry logic in the node MUST use `Backoff` or `retry_with_backoff`.
//! No inline `sleep(Duration::from_secs(30))` retry loops.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Configuration for exponential backoff.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub max_retries: Option<u32>,
}

impl BackoffConfig {
    /// Backoff suitable for SyncEpoch transaction retries (1s → 1h).
    pub fn sync_epoch() -> Self {
        Self {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(3600),
            max_retries: None,
        }
    }

    /// Backoff suitable for spool sync retries (60s → 10min, max 10 attempts).
    pub fn spool_sync() -> Self {
        Self {
            min_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(600),
            max_retries: Some(10),
        }
    }

    /// Backoff suitable for snapshot certification retries (2s → 30s, max 8 attempts).
    pub fn snapshot_certify() -> Self {
        Self {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(30),
            max_retries: Some(8),
        }
    }

    /// Backoff suitable for track recovery retries (30s → 5min).
    pub fn track_recovery() -> Self {
        Self {
            min_delay: Duration::from_secs(30),
            max_delay: Duration::from_secs(300),
            max_retries: None,
        }
    }
}

/// Stateful backoff tracker.
///
/// Each call to `next_delay()` returns an exponentially increasing duration,
/// or `None` if `max_retries` is exceeded.
pub struct Backoff {
    config: BackoffConfig,
    attempt: u32,
    last_attempt: Option<tokio::time::Instant>,
}

impl Backoff {
    pub fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            attempt: 0,
            last_attempt: None,
        }
    }

    /// Compute the next delay. Returns `None` if max_retries exceeded.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if let Some(max) = self.config.max_retries {
            if self.attempt >= max {
                return None;
            }
        }

        let delay = self.config.min_delay * 2u32.saturating_pow(self.attempt);
        let delay = delay.min(self.config.max_delay);
        self.attempt += 1;
        self.last_attempt = Some(tokio::time::Instant::now());
        Some(delay)
    }

    /// Reset backoff state after a success.
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.last_attempt = None;
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns true if enough time has elapsed since the last failure
    /// to warrant another attempt.
    pub fn should_attempt(&self) -> bool {
        match self.last_attempt {
            None => true,
            Some(last) => {
                let backoff =
                    self.config.min_delay * 2u32.saturating_pow(self.attempt.saturating_sub(1));
                let backoff = backoff.min(self.config.max_delay);
                last.elapsed() >= backoff
            }
        }
    }

    /// Record a failure (for use with `should_attempt` polling pattern).
    pub fn record_failure(&mut self) {
        self.last_attempt = Some(tokio::time::Instant::now());
        self.attempt = self.attempt.saturating_add(1);
    }
}

/// Generic retry loop with cancellation support.
///
/// Calls `f` repeatedly with exponential backoff until it succeeds or
/// max_retries is exceeded. Respects the cancellation token between attempts.
pub async fn retry_with_backoff<F, Fut, T, E>(
    config: BackoffConfig,
    cancel: &CancellationToken,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut backoff = Backoff::new(config);

    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                let delay = match backoff.next_delay() {
                    Some(d) => d,
                    None => return Err(e),
                };

                warn!(
                    attempt = backoff.attempt(),
                    delay_secs = delay.as_secs(),
                    error = %e,
                    "retrying after backoff"
                );

                tokio::select! {
                    _ = cancel.cancelled() => return Err(e),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_exponential_growth() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: None,
        };
        let mut b = Backoff::new(config);

        assert_eq!(b.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(b.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(b.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(b.next_delay(), Some(Duration::from_secs(8)));
        assert_eq!(b.attempt(), 4);
    }

    #[test]
    fn backoff_respects_max_delay() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: None,
        };
        let mut b = Backoff::new(config);

        b.next_delay(); // 1
        b.next_delay(); // 2
        b.next_delay(); // 4
        assert_eq!(b.next_delay(), Some(Duration::from_secs(5))); // capped at 5
        assert_eq!(b.next_delay(), Some(Duration::from_secs(5))); // stays capped
    }

    #[test]
    fn backoff_respects_max_retries() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(3),
        };
        let mut b = Backoff::new(config);

        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());
    }

    #[test]
    fn backoff_reset() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(2),
        };
        let mut b = Backoff::new(config);

        b.next_delay();
        b.next_delay();
        assert!(b.next_delay().is_none());

        b.reset();
        assert_eq!(b.attempt(), 0);
        assert!(b.next_delay().is_some());
    }

    #[test]
    fn should_attempt_initially_true() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: None,
        };
        let b = Backoff::new(config);
        assert!(b.should_attempt());
    }

    #[tokio::test]
    async fn retry_succeeds_immediately() {
        let cancel = CancellationToken::new();
        let config = BackoffConfig {
            min_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            max_retries: Some(3),
        };

        let result: Result<i32, String> =
            retry_with_backoff(config, &cancel, || async { Ok(42) }).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retry_succeeds_after_failures() {
        let cancel = CancellationToken::new();
        let config = BackoffConfig {
            min_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            max_retries: Some(5),
        };

        let mut call_count = 0u32;
        let result: Result<i32, String> = retry_with_backoff(config, &cancel, || {
            call_count += 1;
            let count = call_count;
            async move {
                if count < 3 {
                    Err(format!("fail {}", count))
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count, 3);
    }

    #[tokio::test]
    async fn retry_exhausts_retries() {
        let cancel = CancellationToken::new();
        let config = BackoffConfig {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            max_retries: Some(2),
        };

        let result: Result<i32, String> =
            retry_with_backoff(config, &cancel, || async { Err("always fails".to_string()) }).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_respects_cancellation() {
        let cancel = CancellationToken::new();
        let config = BackoffConfig {
            min_delay: Duration::from_secs(100),
            max_delay: Duration::from_secs(100),
            max_retries: None,
        };

        // Cancel after a short delay
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel2.cancel();
        });

        let result: Result<i32, String> =
            retry_with_backoff(config, &cancel, || async { Err("fail".to_string()) }).await;

        assert!(result.is_err());
    }

    #[test]
    fn preset_configs() {
        let se = BackoffConfig::sync_epoch();
        assert_eq!(se.min_delay, Duration::from_secs(1));
        assert_eq!(se.max_delay, Duration::from_secs(3600));
        assert!(se.max_retries.is_none());

        let ss = BackoffConfig::spool_sync();
        assert_eq!(ss.max_retries, Some(10));

        let tr = BackoffConfig::track_recovery();
        assert_eq!(tr.min_delay, Duration::from_secs(30));
    }
}
