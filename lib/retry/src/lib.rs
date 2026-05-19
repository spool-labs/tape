//! Shared retry infrastructure with exponential backoff.
//!
//! All retry logic MUST use `Backoff`, `retry`, or `retry_if`.
//! No inline `sleep(Duration::from_secs(30))` retry loops.

use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Configuration for exponential backoff.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_retries: Option<u32>,
}

impl RetryConfig {
    /// No retries, 0s base, 0s max.
    /// Useful for disabling retries in tests
    pub fn none() -> Self {
        Self {
            base_delay: Duration::from_secs(0),
            max_delay: Duration::from_secs(0),
            max_retries: Some(0),
        }
    }

    /// 3 retries, 1s base, 5s max.
    pub fn three() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: Some(3),
        }
    }

    /// 10 retries, 1s base, 5s max.
    pub fn ten() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: Some(10),
        }
    }

    /// Unlimited retries, 500ms base, 5s max.
    pub fn infinite() -> Self {
        Self {
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
            max_retries: None,
        }
    }
}

/// Errors that can classify themselves as retryable or permanent.
pub trait Retryable {
    fn is_retryable(&self) -> bool;
}

/// Stateful delay calculator for manual loops (ingestor, RPC client).
///
/// Each call to `next_delay()` returns an exponentially increasing duration
/// with half-jitter, or `None` if `max_retries` is exceeded.
pub struct Backoff {
    config: RetryConfig,
    attempt: u32,
}

impl Backoff {
    pub fn new(config: RetryConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Compute the next delay with half-jitter. Returns `None` if max retries exceeded.
    ///
    /// Half-jitter: `delay = uniform(base/2, base)` where `base = min(max_delay, base_delay * 2^attempt)`.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if let Some(max) = self.config.max_retries {
            if self.attempt >= max {
                return None;
            }
        }

        let delay = compute_delay(&self.config, self.attempt);
        self.attempt += 1;
        Some(delay)
    }

    /// Reset backoff state after a success.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// Compute a single delay with half-jitter for the given attempt.
pub fn compute_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let base = config.base_delay * 2u32.saturating_pow(attempt);
    let base = base.min(config.max_delay);

    let half = base / 2;
    let jitter = Duration::from_millis(
        rand::thread_rng().gen_range(0..=half.as_millis().max(1) as u64),
    );
    half + jitter
}

/// Sleep for the next backoff delay, or return `true` if cancelled.
pub async fn backoff_or_cancel(
    backoff: &mut Backoff, 
    cancel: &CancellationToken
    ) -> bool {

    if let Some(delay) = backoff.next_delay() {
        tokio::select! {
            _ = cancel.cancelled() => true,
            _ = tokio::time::sleep(delay) => false,
        }
    } else {
        true
    }
}

/// Retry all errors with exponential backoff.
pub async fn retry<F, Fut, T, E>(
    config: RetryConfig,
    cancel: Option<&CancellationToken>,
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
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "retrying after backoff"
                );

                match cancel {
                    Some(token) => {
                        tokio::select! {
                            _ = token.cancelled() => return Err(e),
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                    None => tokio::time::sleep(delay).await,
                }
            }
        }
    }
}

/// Retry only when `should_retry` returns true.
pub async fn retry_if<F, Fut, T, E>(
    config: RetryConfig,
    cancel: Option<&CancellationToken>,
    mut f: F,
    should_retry: impl Fn(&E) -> bool,
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
                if !should_retry(&e) {
                    return Err(e);
                }

                let delay = match backoff.next_delay() {
                    Some(d) => d,
                    None => return Err(e),
                };

                warn!(
                    attempt = backoff.attempt(),
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "retrying after backoff"
                );

                match cancel {
                    Some(token) => {
                        tokio::select! {
                            _ = token.cancelled() => return Err(e),
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                    None => tokio::time::sleep(delay).await,
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
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: None,
        };
        let mut b = Backoff::new(config);

        let d = b.next_delay().unwrap(); // base=1s
        assert!(d >= Duration::from_millis(500));
        assert!(d <= Duration::from_secs(1));

        let d = b.next_delay().unwrap(); // base=2s
        assert!(d >= Duration::from_secs(1));
        assert!(d <= Duration::from_secs(2));

        let d = b.next_delay().unwrap(); // base=4s
        assert!(d >= Duration::from_secs(2));
        assert!(d <= Duration::from_secs(4));

        let d = b.next_delay().unwrap(); // base=8s
        assert!(d >= Duration::from_secs(4));
        assert!(d <= Duration::from_secs(8));

        assert_eq!(b.attempt(), 4);
    }

    #[test]
    fn backoff_respects_max_delay() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: None,
        };
        let mut b = Backoff::new(config);

        b.next_delay(); // base=1s
        b.next_delay(); // base=2s
        b.next_delay(); // base=4s
        let d = b.next_delay().unwrap(); // base=5s (capped)
        assert!(d >= Duration::from_millis(2500));
        assert!(d <= Duration::from_secs(5));
        let d = b.next_delay().unwrap(); // stays capped
        assert!(d >= Duration::from_millis(2500));
        assert!(d <= Duration::from_secs(5));
    }

    #[test]
    fn jitter_bounded() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            max_retries: None,
        };

        for _ in 0..20 {
            let mut b = Backoff::new(config.clone());
            let d = b.next_delay().unwrap(); // base=10s
            assert!(d >= Duration::from_secs(5));
            assert!(d <= Duration::from_secs(10));
        }
    }

    #[test]
    fn backoff_respects_max_retries() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
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
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
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
    fn presets() {
        let ten = RetryConfig::ten();
        assert_eq!(ten.max_retries, Some(10));
        assert_eq!(ten.base_delay, Duration::from_secs(1));
        assert_eq!(ten.max_delay, Duration::from_secs(5));

        let inf = RetryConfig::infinite();
        assert_eq!(inf.max_retries, None);
        assert_eq!(inf.base_delay, Duration::from_millis(500));
        assert_eq!(inf.max_delay, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn retry_succeeds_immediately() {
        let result: Result<i32, String> =
            retry(RetryConfig::ten(), None, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retry_succeeds_after_failures() {
        let mut call_count = 0u32;
        let result: Result<i32, String> = retry(RetryConfig::ten(), None, || {
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
        let config = RetryConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            max_retries: Some(2),
        };

        let result: Result<i32, String> =
            retry(config, None, || async { Err("always fails".to_string()) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_respects_cancellation() {
        let cancel = CancellationToken::new();
        let config = RetryConfig {
            base_delay: Duration::from_secs(100),
            max_delay: Duration::from_secs(100),
            max_retries: None,
        };

        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel2.cancel();
        });

        let result: Result<i32, String> =
            retry(config, Some(&cancel), || async { Err("fail".to_string()) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_if_skips_non_retryable() {
        let mut call_count = 0u32;
        let result: Result<i32, String> = retry_if(
            RetryConfig::ten(),
            None,
            || {
                call_count += 1;
                async { Err("permanent".to_string()) }
            },
            |e: &String| e != "permanent",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(call_count, 1);
    }

    #[tokio::test]
    async fn retry_if_retries_retryable() {
        let config = RetryConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            max_retries: Some(5),
        };

        let mut call_count = 0u32;
        let result: Result<i32, String> = retry_if(
            config,
            None,
            || {
                call_count += 1;
                let count = call_count;
                async move {
                    if count < 3 {
                        Err("transient".to_string())
                    } else {
                        Ok(42)
                    }
                }
            },
            |e: &String| e == "transient",
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count, 3);
    }

    #[test]
    fn compute_delay_bounded() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(3),
        };

        let d = compute_delay(&config, 0); // base=1s
        assert!(d >= Duration::from_millis(500));
        assert!(d <= Duration::from_secs(1));

        let d = compute_delay(&config, 1); // base=2s
        assert!(d >= Duration::from_secs(1));
        assert!(d <= Duration::from_secs(2));

        let d = compute_delay(&config, 10); // capped at 60s
        assert!(d >= Duration::from_secs(30));
        assert!(d <= Duration::from_secs(60));
    }
}
