//! Wait/polling utilities for e2e testing.
//!
//! Provides async functions that poll for various conditions to be met,
//! with configurable timeouts and retry logic.

use std::future::Future;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::Tapedrive;

/// Default poll interval.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Default timeout for most operations.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Long timeout for operations that may take a while.
pub const LONG_TIMEOUT: Duration = Duration::from_secs(120);

/// Wait for a condition to be true.
///
/// # Arguments
///
/// * `condition` - Async function that returns `Result<bool>`
/// * `timeout` - Maximum time to wait
/// * `poll_interval` - Time between polls
///
/// # Example
///
/// ```ignore
/// wait_for(
///     || async { Ok(node.is_healthy().await) },
///     Duration::from_secs(30),
///     Duration::from_millis(500),
/// ).await?;
/// ```
pub async fn wait_for<F, Fut>(
    condition: F,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            bail!("Timeout after {:?}", timeout);
        }

        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => {
                // Log but continue - condition might become true after error clears
                tracing::debug!("Condition check failed: {}", e);
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for a condition with a description for error messages.
pub async fn wait_for_with_desc<F, Fut>(
    desc: &str,
    condition: F,
    timeout: Duration,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            bail!("Timeout waiting for '{}' after {:?}", desc, timeout);
        }

        match condition().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => {
                tracing::debug!("Condition '{}' check failed: {}", desc, e);
            }
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// Wait for a specific epoch phase.
///
/// # Arguments
///
/// * `cli` - CLI wrapper
/// * `phase` - Expected phase ("Active", "Syncing", "Settling")
/// * `timeout` - Maximum time to wait
pub async fn wait_for_epoch_phase(
    cli: &Tapedrive,
    phase: &str,
    timeout: Duration,
) -> Result<()> {
    wait_for_with_desc(
        &format!("epoch phase = {}", phase),
        || async {
            match cli.account_epoch() {
                Ok(epoch) => Ok(epoch.phase.as_deref() == Some(phase)),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for epoch to reach a specific ID.
pub async fn wait_for_epoch_id(
    cli: &Tapedrive,
    epoch_id: u64,
    timeout: Duration,
) -> Result<()> {
    wait_for_with_desc(
        &format!("epoch id = {}", epoch_id),
        || async {
            match cli.account_epoch() {
                Ok(epoch) => Ok(epoch.id == Some(epoch_id)),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for committee to reach a minimum size.
pub async fn wait_for_committee_size(
    cli: &Tapedrive,
    min_size: usize,
    timeout: Duration,
) -> Result<()> {
    wait_for_with_desc(
        &format!("committee size >= {}", min_size),
        || async {
            match cli.account_system() {
                Ok(system) => Ok(system.committee_size.unwrap_or(0) >= min_size),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for committee_next to reach a minimum size.
pub async fn wait_for_committee_next_size(
    cli: &Tapedrive,
    min_size: usize,
    timeout: Duration,
) -> Result<()> {
    wait_for_with_desc(
        &format!("committee_next size >= {}", min_size),
        || async {
            match cli.account_system() {
                Ok(system) => Ok(system.committee_next_size.unwrap_or(0) >= min_size),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for a node to be healthy via HTTP health check.
pub async fn wait_for_node_health(url: &str, timeout: Duration) -> Result<()> {
    let health_url = format!("{}/v1/health", url.trim_end_matches('/'));
    let client = reqwest::Client::new();

    wait_for_with_desc(
        &format!("node healthy at {}", url),
        || {
            let client = client.clone();
            let health_url = health_url.clone();
            async move {
                match client
                    .get(&health_url)
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                {
                    Ok(resp) => Ok(resp.status().is_success()),
                    Err(_) => Ok(false),
                }
            }
        },
        timeout,
    )
    .await
}

/// Wait for multiple nodes to be healthy.
pub async fn wait_for_nodes_health(urls: &[String], timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    let remaining = |start: std::time::Instant| {
        timeout
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO)
    };

    for url in urls {
        wait_for_node_health(url, remaining(start)).await?;
    }

    Ok(())
}

/// Wait for a track to be certified.
///
/// Note: This requires the track_id format and may need adjustment
/// based on actual CLI output.
pub async fn wait_for_track_certified(
    _cli: &Tapedrive,
    _authority: &solana_sdk::pubkey::Pubkey,
    _key_hash: &str,
    timeout: Duration,
) -> Result<()> {
    // TODO: Implement when track status CLI output is stable
    wait_for_with_desc(
        "track certified",
        || async { Ok(false) }, // Placeholder
        timeout,
    )
    .await
}

/// Wait for RPC to be ready.
pub async fn wait_for_rpc(rpc_url: &str, timeout: Duration) -> Result<()> {
    let client = reqwest::Client::new();

    wait_for_with_desc(
        &format!("RPC ready at {}", rpc_url),
        || {
            let client = client.clone();
            let rpc_url = rpc_url.to_string();
            async move {
                let result = client
                    .post(&rpc_url)
                    .json(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getHealth"
                    }))
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await;

                match result {
                    Ok(resp) => Ok(resp.status().is_success()),
                    Err(_) => Ok(false),
                }
            }
        },
        timeout,
    )
    .await
}

/// Retry an operation with exponential backoff.
///
/// # Arguments
///
/// * `operation` - Async function to retry
/// * `max_attempts` - Maximum number of attempts
/// * `initial_delay` - Initial delay between attempts
///
/// # Returns
///
/// The result of the first successful attempt, or the last error.
pub async fn retry_with_backoff<F, Fut, T>(
    operation: F,
    max_attempts: u32,
    initial_delay: Duration,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut delay = initial_delay;
    let mut last_error = None;

    for attempt in 0..max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No attempts made")))
}

/// Sleep for a duration with progress indication.
///
/// Useful for long waits where you want to show progress.
pub async fn sleep_with_progress(duration: Duration, label: &str) {
    let steps = 10;
    let step_duration = duration / steps;

    for i in 0..steps {
        tokio::time::sleep(step_duration).await;
        tracing::info!(
            "{}: {}/{}",
            label,
            i + 1,
            steps
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_immediate() {
        let result = wait_for(
            || async { Ok(true) },
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_timeout() {
        let result = wait_for(
            || async { Ok(false) },
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_retry_with_backoff() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            || {
                let counter = counter_clone.clone();
                async move {
                    let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if count < 2 {
                        Err(anyhow::anyhow!("Not yet"))
                    } else {
                        Ok("success")
                    }
                }
            },
            5,
            Duration::from_millis(10),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
