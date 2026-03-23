use serde::Deserialize;

/// Recovery subsystem resource-shaping settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RecoveryConfig {
    /// Maximum number of spool workers that may run concurrently.
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,

    /// Page size for spool sync requests.
    #[serde(default = "default_sync_batch")]
    pub sync_batch: usize,

    /// Batch size for local scan passes.
    #[serde(default = "default_scan_batch")]
    pub scan_batch: usize,

    /// Batch size for pending repair work.
    #[serde(default = "default_repair_batch")]
    pub repair_batch: usize,

    /// Batch size for pending recovery work.
    #[serde(default = "default_recover_batch")]
    pub recover_batch: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_workers: default_max_workers(),
            sync_batch: default_sync_batch(),
            scan_batch: default_scan_batch(),
            repair_batch: default_repair_batch(),
            recover_batch: default_recover_batch(),
        }
    }
}

fn default_max_workers() -> usize {
    50
}

fn default_sync_batch() -> usize {
    100
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
