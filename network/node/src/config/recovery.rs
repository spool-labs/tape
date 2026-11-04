use serde::Deserialize;
use tape_core::types::StorageUnits;

/// Recovery subsystem resource-shaping settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Maximum number of spool workers that may run concurrently.
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,

    /// Batch size for local scan passes.
    #[serde(default = "default_scan_batch")]
    pub scan_batch: usize,

    /// Batch size for pending repair work.
    #[serde(default = "default_repair_batch")]
    pub repair_batch: usize,

    /// Batch size for pending recovery work.
    #[serde(default = "default_recover_batch")]
    pub recover_batch: usize,

    /// Recovery rows per stored spool below which a failed repair goes to recovery.
    #[serde(default = "default_recover_queue_limit")]
    pub recover_queue_limit: usize,

    /// Seconds every repair helper request gets before its bytes are counted.
    #[serde(default = "default_repair_deadline_base_secs")]
    pub repair_deadline_base_secs: u64,

    /// Bytes per second a repair helper must keep up, on top of the base.
    #[serde(default = "default_repair_floor_rate")]
    pub repair_floor_rate: StorageUnits,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_workers: default_max_workers(),
            scan_batch: default_scan_batch(),
            repair_batch: default_repair_batch(),
            recover_batch: default_recover_batch(),
            recover_queue_limit: default_recover_queue_limit(),
            repair_deadline_base_secs: default_repair_deadline_base_secs(),
            repair_floor_rate: default_repair_floor_rate(),
        }
    }
}

fn default_max_workers() -> usize {
    50
}

fn default_scan_batch() -> usize {
    100
}

fn default_repair_batch() -> usize {
    10
}

fn default_recover_batch() -> usize {
    10
}

fn default_recover_queue_limit() -> usize {
    16
}

fn default_repair_deadline_base_secs() -> u64 {
    3
}

fn default_repair_floor_rate() -> StorageUnits {
    StorageUnits::mb(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // an empty section takes the repair limits' defaults, and each one can be set
    #[test]
    fn repair_limits() {
        let config: RecoveryConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config, RecoveryConfig::default());
        assert_eq!(config.recover_queue_limit, 16);
        assert_eq!(config.repair_deadline_base_secs, 3);
        assert_eq!(config.repair_floor_rate, StorageUnits::mb(1));

        let yaml = "recover_queue_limit: 4\nrepair_deadline_base_secs: 5\nrepair_floor_rate: 2048";
        let config: RecoveryConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.recover_queue_limit, 4);
        assert_eq!(config.repair_deadline_base_secs, 5);
        assert_eq!(config.repair_floor_rate, StorageUnits::from_bytes(2048));
    }
}
