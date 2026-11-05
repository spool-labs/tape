use serde::Deserialize;
use tape_core::types::StorageUnits;

/// The spool manager's limits and waits
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Most fetches running at once
    #[serde(default = "default_max_fetches")]
    pub max_fetches: usize,

    /// Most bytes being fetched at once
    #[serde(default = "default_max_fetch_bytes")]
    pub max_fetch_bytes: StorageUnits,

    /// Most of the running fetches that may be recoveries
    #[serde(default = "default_max_recoveries")]
    pub max_recoveries: usize,

    /// Most of our fetches one peer serves at once
    #[serde(default = "default_max_peer_fetches")]
    pub max_peer_fetches: usize,

    /// Most tracks one fetch asks for
    #[serde(default = "default_fetch_tracks")]
    pub fetch_tracks: usize,

    /// Most bytes one fetch asks for
    #[serde(default = "default_fetch_bytes")]
    pub fetch_bytes: StorageUnits,

    /// Seconds every fetch gets before it gives up
    #[serde(default = "default_fetch_timeout_secs")]
    pub fetch_timeout_secs: u64,

    /// Seconds a fetch gets on top for each MiB it asked for
    #[serde(default = "default_fetch_secs_per_mib")]
    pub fetch_secs_per_mib: u64,

    /// Most entries one turn looks at
    #[serde(default = "default_turn_entries")]
    pub turn_entries: usize,

    /// Tracks filling reads each time the cursor goes round
    #[serde(default = "default_fill_page")]
    pub fill_page: usize,

    /// Most turns one wake does
    #[serde(default = "default_turns_per_wake")]
    pub turns_per_wake: usize,

    /// Seconds the loop sleeps at most between wakes
    #[serde(default = "default_wake_secs")]
    pub wake_secs: u64,

    /// Seconds a peer rests after its first failure in a row
    #[serde(default = "default_peer_rest_secs")]
    pub peer_rest_secs: u64,

    /// Longest rest a peer gets for failing
    #[serde(default = "default_peer_rest_max_secs")]
    pub peer_rest_max_secs: u64,

    /// Seconds a peer rests after it sent wrong data
    #[serde(default = "default_wrong_data_rest_secs")]
    pub wrong_data_rest_secs: u64,

    /// Seconds of failing in a row after which a peer is gone
    #[serde(default = "default_peer_gone_secs")]
    pub peer_gone_secs: u64,

    /// Seconds of failing repairs after which a track may go to recovery
    #[serde(default = "default_repair_failing_secs")]
    pub repair_failing_secs: u64,

    /// Seconds a track waits after its first failure of its own
    #[serde(default = "default_track_wait_secs")]
    pub track_wait_secs: u64,

    /// Longest wait a track gets for failing on its own
    #[serde(default = "default_track_wait_max_secs")]
    pub track_wait_max_secs: u64,

    /// Epochs a node keeps a spool's slices after it loses the spool
    #[serde(default = "default_keep_epochs")]
    pub keep_epochs: u64,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_fetches: default_max_fetches(),
            max_fetch_bytes: default_max_fetch_bytes(),
            max_recoveries: default_max_recoveries(),
            max_peer_fetches: default_max_peer_fetches(),
            fetch_tracks: default_fetch_tracks(),
            fetch_bytes: default_fetch_bytes(),
            fetch_timeout_secs: default_fetch_timeout_secs(),
            fetch_secs_per_mib: default_fetch_secs_per_mib(),
            turn_entries: default_turn_entries(),
            fill_page: default_fill_page(),
            turns_per_wake: default_turns_per_wake(),
            wake_secs: default_wake_secs(),
            peer_rest_secs: default_peer_rest_secs(),
            peer_rest_max_secs: default_peer_rest_max_secs(),
            wrong_data_rest_secs: default_wrong_data_rest_secs(),
            peer_gone_secs: default_peer_gone_secs(),
            repair_failing_secs: default_repair_failing_secs(),
            track_wait_secs: default_track_wait_secs(),
            track_wait_max_secs: default_track_wait_max_secs(),
            keep_epochs: default_keep_epochs(),
        }
    }
}

fn default_max_fetches() -> usize {
    16
}

fn default_max_fetch_bytes() -> StorageUnits {
    StorageUnits::mb(128)
}

fn default_max_recoveries() -> usize {
    4
}

fn default_max_peer_fetches() -> usize {
    2
}

fn default_fetch_tracks() -> usize {
    64
}

fn default_fetch_bytes() -> StorageUnits {
    StorageUnits::mb(8)
}

fn default_fetch_timeout_secs() -> u64 {
    5
}

fn default_fetch_secs_per_mib() -> u64 {
    1
}

fn default_turn_entries() -> usize {
    128
}

fn default_fill_page() -> usize {
    512
}

fn default_turns_per_wake() -> usize {
    8
}

fn default_wake_secs() -> u64 {
    1
}

fn default_peer_rest_secs() -> u64 {
    5
}

fn default_peer_rest_max_secs() -> u64 {
    300
}

fn default_wrong_data_rest_secs() -> u64 {
    1_800
}

fn default_peer_gone_secs() -> u64 {
    600
}

fn default_repair_failing_secs() -> u64 {
    300
}

fn default_track_wait_secs() -> u64 {
    2
}

fn default_track_wait_max_secs() -> u64 {
    300
}

fn default_keep_epochs() -> u64 {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    // an empty section takes the design's values, and each one can be set
    #[test]
    fn spool_limits() {
        let config: RecoveryConfig = serde_yaml::from_str("{}").expect("empty section");
        assert_eq!(config, RecoveryConfig::default());
        assert_eq!(config.max_fetch_bytes, StorageUnits::mb(128));
        assert_eq!(config.repair_failing_secs, 300);

        let yaml = "max_fetches: 4\nfetch_bytes: 2048\nkeep_epochs: 6";
        let config: RecoveryConfig = serde_yaml::from_str(yaml).expect("section");
        assert_eq!(config.max_fetches, 4);
        assert_eq!(config.fetch_bytes, StorageUnits::from_bytes(2048));
        assert_eq!(config.keep_epochs, 6);
    }
}
