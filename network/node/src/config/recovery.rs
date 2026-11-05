use std::time::Duration;

use serde::Deserialize;
use tape_core::types::StorageUnits;
use tape_recover::limits::SpoolLimits;

/// The spool manager's limits and waits, as node.yaml sets them. A field
/// left out keeps the value of `SpoolLimits::default()`
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RecoveryConfig {
    /// Most fetches running at once
    pub max_fetches: usize,
    /// Most bytes being fetched at once
    pub max_fetch_bytes: StorageUnits,
    /// Most of the running fetches that may be recoveries
    pub max_recoveries: usize,
    /// Most of our fetches one peer serves at once
    pub max_peer_fetches: usize,
    /// Most tracks one fetch asks for
    pub fetch_tracks: usize,
    /// Most bytes one fetch asks for
    pub fetch_bytes: StorageUnits,
    /// Seconds every fetch gets before it gives up
    pub fetch_timeout_secs: u64,
    /// Seconds a fetch gets on top for each MiB it asked for
    pub fetch_secs_per_mib: u64,
    /// Most missing tracks one turn looks at
    pub turn_tracks: usize,
    /// Tracks the scan adds each time the cursor goes round
    pub scan_page: usize,
    /// Most turns one wake takes
    pub turns_per_wake: usize,
    /// Seconds the loop sleeps at most between wakes
    pub wake_secs: u64,
    /// Seconds a peer rests after its first failure in a row
    pub peer_rest_secs: u64,
    /// Longest rest a peer gets for failing
    pub peer_rest_max_secs: u64,
    /// Seconds a peer rests after it sent wrong data
    pub wrong_data_rest_secs: u64,
    /// Seconds of failing in a row after which a peer is gone
    pub peer_gone_secs: u64,
    /// Seconds of failing repairs after which a track may go to recovery
    pub repair_failing_secs: u64,
    /// Seconds a track waits after its first failure of its own
    pub track_wait_secs: u64,
    /// Longest wait a track gets for failing on its own
    pub track_wait_max_secs: u64,
    /// Epochs a node keeps a spool's slices after it loses the spool
    pub keep_epochs: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self::from(SpoolLimits::default())
    }
}

impl From<SpoolLimits> for RecoveryConfig {
    fn from(limits: SpoolLimits) -> Self {
        Self {
            max_fetches: limits.max_fetches,
            max_fetch_bytes: limits.max_fetch_bytes,
            max_recoveries: limits.max_recoveries,
            max_peer_fetches: limits.max_peer_fetches,
            fetch_tracks: limits.fetch_tracks,
            fetch_bytes: limits.fetch_bytes,
            fetch_timeout_secs: limits.fetch_timeout.as_secs(),
            fetch_secs_per_mib: limits.fetch_time_per_mib.as_secs(),
            turn_tracks: limits.turn_tracks,
            scan_page: limits.scan_page,
            turns_per_wake: limits.turns_per_wake,
            wake_secs: limits.wake.as_secs(),
            peer_rest_secs: limits.peer_rest.as_secs(),
            peer_rest_max_secs: limits.peer_rest_max.as_secs(),
            wrong_data_rest_secs: limits.wrong_data_rest.as_secs(),
            peer_gone_secs: limits.peer_gone.as_secs(),
            repair_failing_secs: limits.repair_failing.as_secs(),
            track_wait_secs: limits.track_wait.as_secs(),
            track_wait_max_secs: limits.track_wait_max.as_secs(),
            keep_epochs: limits.keep_epochs,
        }
    }
}

impl From<&RecoveryConfig> for SpoolLimits {
    fn from(config: &RecoveryConfig) -> Self {
        let secs = Duration::from_secs;
        Self {
            max_fetches: config.max_fetches,
            max_fetch_bytes: config.max_fetch_bytes,
            max_recoveries: config.max_recoveries,
            max_peer_fetches: config.max_peer_fetches,
            fetch_tracks: config.fetch_tracks,
            fetch_bytes: config.fetch_bytes,
            fetch_timeout: secs(config.fetch_timeout_secs),
            fetch_time_per_mib: secs(config.fetch_secs_per_mib),
            turn_tracks: config.turn_tracks,
            scan_page: config.scan_page,
            turns_per_wake: config.turns_per_wake,
            wake: secs(config.wake_secs),
            peer_rest: secs(config.peer_rest_secs),
            peer_rest_max: secs(config.peer_rest_max_secs),
            wrong_data_rest: secs(config.wrong_data_rest_secs),
            peer_gone: secs(config.peer_gone_secs),
            repair_failing: secs(config.repair_failing_secs),
            track_wait: secs(config.track_wait_secs),
            track_wait_max: secs(config.track_wait_max_secs),
            keep_epochs: config.keep_epochs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // an empty section takes the preset's values, and each one can be set
    #[test]
    fn spool_limits() {
        let config: RecoveryConfig = serde_yaml::from_str("{}").expect("empty section");
        assert_eq!(config, RecoveryConfig::default());
        assert_eq!(SpoolLimits::from(&config), SpoolLimits::default());
        assert_eq!(config.max_fetch_bytes, StorageUnits::mb(128));
        assert_eq!(config.repair_failing_secs, 300);

        let yaml = "max_fetches: 4\nfetch_bytes: 2048\nkeep_epochs: 6";
        let config: RecoveryConfig = serde_yaml::from_str(yaml).expect("section");
        assert_eq!(config.max_fetches, 4);
        assert_eq!(config.fetch_bytes, StorageUnits::from_bytes(2048));
        assert_eq!(config.keep_epochs, 6);
        assert_eq!(config.scan_page, SpoolLimits::default().scan_page);
    }
}
