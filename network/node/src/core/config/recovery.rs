use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// Recovery subsystem configuration parameters.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum concurrent track sync tasks.
    pub max_concurrent_track_syncs: usize,
    /// Maximum concurrent slice downloads across all tracks.
    pub max_concurrent_slice_syncs: usize,
    /// Maximum queued recovery tasks before backpressure.
    pub recovery_track_concurrency: usize,
    /// Maximum concurrent spool sync operations.
    pub spool_sync_concurrency: usize,
    /// Timeout for individual repair requests to helpers.
    pub repair_request_timeout: Duration,
    /// Timeout for individual slice download requests.
    pub slice_request_timeout: Duration,
    /// Timeout for metadata requests to peers.
    pub metadata_request_timeout: Duration,
    /// Total timeout before spool sync falls back to direct recovery.
    pub spool_sync_recovery_timeout: Duration,
    /// Maximum time to defer live uploads during recovery.
    pub max_total_defer: Duration,
    /// Delay between track sync retry attempts.
    pub track_sync_retry_delay: Duration,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_concurrent_track_syncs: 100,
            max_concurrent_slice_syncs: 2000,
            recovery_track_concurrency: 1000,
            spool_sync_concurrency: 10,
            repair_request_timeout: Duration::from_secs(45),
            slice_request_timeout: Duration::from_secs(45),
            metadata_request_timeout: Duration::from_secs(5),
            spool_sync_recovery_timeout: Duration::from_secs(12 * 3600),
            max_total_defer: Duration::from_secs(120),
            track_sync_retry_delay: Duration::from_secs(30),
        }
    }
}

/// Recovery config loaded from YAML before defaults are applied.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RawRecoveryConfig {
    #[serde(default)]
    pub max_concurrent_track_syncs: Option<usize>,
    #[serde(default)]
    pub max_concurrent_slice_syncs: Option<usize>,
    #[serde(default)]
    pub recovery_track_concurrency: Option<usize>,
    #[serde(default)]
    pub spool_sync_concurrency: Option<usize>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub repair_request_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub slice_request_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub metadata_request_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub spool_sync_recovery_timeout: Option<Duration>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub max_total_defer: Option<Duration>,
    #[serde(default, deserialize_with = "duration_secs_opt")]
    pub track_sync_retry_delay: Option<Duration>,
}

impl RawRecoveryConfig {
    pub(crate) fn build(self) -> RecoveryConfig {
        let defaults = RecoveryConfig::default();

        RecoveryConfig {
            max_concurrent_track_syncs: self
                .max_concurrent_track_syncs
                .unwrap_or(defaults.max_concurrent_track_syncs),
            max_concurrent_slice_syncs: self
                .max_concurrent_slice_syncs
                .unwrap_or(defaults.max_concurrent_slice_syncs),
            recovery_track_concurrency: self
                .recovery_track_concurrency
                .unwrap_or(defaults.recovery_track_concurrency),
            spool_sync_concurrency: self
                .spool_sync_concurrency
                .unwrap_or(defaults.spool_sync_concurrency),
            repair_request_timeout: self
                .repair_request_timeout
                .unwrap_or(defaults.repair_request_timeout),
            slice_request_timeout: self
                .slice_request_timeout
                .unwrap_or(defaults.slice_request_timeout),
            metadata_request_timeout: self
                .metadata_request_timeout
                .unwrap_or(defaults.metadata_request_timeout),
            spool_sync_recovery_timeout: self
                .spool_sync_recovery_timeout
                .unwrap_or(defaults.spool_sync_recovery_timeout),
            max_total_defer: self.max_total_defer.unwrap_or(defaults.max_total_defer),
            track_sync_retry_delay: self
                .track_sync_retry_delay
                .unwrap_or(defaults.track_sync_retry_delay),
        }
    }
}

fn duration_secs_opt<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<u64>::deserialize(deserializer).map(|secs| secs.map(Duration::from_secs))
}
