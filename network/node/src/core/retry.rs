use std::time::Duration;

use rand::Rng;

use crate::core::BackoffConfig;
use crate::core::task::TaskCategory;

pub fn backoff_for(category: TaskCategory) -> BackoffConfig {
    match category {
        TaskCategory::SolanaTx => BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(20),
        },
        TaskCategory::PeerHttp => BackoffConfig {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(300),
            max_retries: Some(50),
        },
        TaskCategory::CpuHeavy => BackoffConfig {
            min_delay: Duration::from_secs(30),
            max_delay: Duration::from_secs(300),
            max_retries: None,
        },
        TaskCategory::Internal => BackoffConfig {
            min_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
            max_retries: Some(10),
        },
    }
}

pub fn compute_delay(config: &BackoffConfig, attempt: u32) -> Option<Duration> {
    if let Some(max) = config.max_retries {
        if attempt >= max {
            return None;
        }
    }
    let base = config.min_delay * 2u32.saturating_pow(attempt);
    let base = base.min(config.max_delay);
    let half = base / 2;
    let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..=half.as_millis() as u64));
    Some(half + jitter)
}
