use serde::{Deserialize, Serialize};
use solana_sdk::commitment_config::CommitmentLevel;
use std::time::Duration;

/// RPC client configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcConfig {
    /// Primary and fallback RPC endpoints
    pub endpoints: Vec<String>,

    /// Commitment level for queries
    #[serde(default = "default_commitment")]
    pub commitment: CommitmentLevel,

    /// Request timeout per attempt
    #[serde(
        default = "default_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Duration,

    /// Retry policy configuration
    #[serde(default)]
    pub retry: RpcRetryConfig,
}

/// Retry and backoff configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcRetryConfig {
    /// Maximum retry attempts (default: 5)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Minimum backoff delay (default: 500ms)
    #[serde(
        default = "default_min_backoff",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub min_backoff: Duration,

    /// Maximum backoff delay (default: 30s)
    #[serde(
        default = "default_max_backoff",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub max_backoff: Duration,

    /// Whether to add jitter (default: true)
    #[serde(default = "default_jitter")]
    pub jitter: bool,

    /// Max endpoints to try before giving up (default: 3)
    #[serde(default = "default_max_endpoint_attempts")]
    pub max_endpoint_attempts: u32,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
            commitment: default_commitment(),
            timeout: default_timeout(),
            retry: RpcRetryConfig::default(),
        }
    }
}

impl RpcRetryConfig {
    /// Convert to `tape_retry::RetryConfig` for use with `Backoff`.
    pub fn to_retry_config(&self) -> tape_retry::RetryConfig {
        tape_retry::RetryConfig {
            base_delay: self.min_backoff,
            max_delay: self.max_backoff,
            max_retries: Some(self.max_retries),
        }
    }
}

impl Default for RpcRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            min_backoff: default_min_backoff(),
            max_backoff: default_max_backoff(),
            jitter: default_jitter(),
            max_endpoint_attempts: default_max_endpoint_attempts(),
        }
    }
}

// Default value functions
fn default_commitment() -> CommitmentLevel {
    CommitmentLevel::Finalized
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_retries() -> u32 {
    5
}

fn default_min_backoff() -> Duration {
    Duration::from_millis(500)
}

fn default_max_backoff() -> Duration {
    Duration::from_secs(30)
}

fn default_jitter() -> bool {
    true
}

fn default_max_endpoint_attempts() -> u32 {
    3
}

// Custom serialization/deserialization for Duration
fn serialize_duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u64(duration.as_millis() as u64)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis = u64::deserialize(deserializer)?;
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RpcConfig::default();
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.commitment, CommitmentLevel::Finalized);
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.retry.max_retries, 5);
    }

    #[test]
    fn test_serialize_deserialize() {
        let config = RpcConfig {
            endpoints: vec!["https://test.com".to_string()],
            commitment: CommitmentLevel::Finalized,
            timeout: Duration::from_secs(10),
            retry: RpcRetryConfig {
                max_retries: 3,
                min_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                jitter: false,
                max_endpoint_attempts: 2,
            },
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RpcConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.endpoints, deserialized.endpoints);
        assert_eq!(config.commitment, deserialized.commitment);
        assert_eq!(config.timeout, deserialized.timeout);
        assert_eq!(config.retry.max_retries, deserialized.retry.max_retries);
    }

    #[test]
    fn test_default_retry_config() {
        let config = RpcRetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.min_backoff, Duration::from_millis(500));
        assert_eq!(config.max_backoff, Duration::from_secs(30));
        assert!(config.jitter);
        assert_eq!(config.max_endpoint_attempts, 3);
    }
}
