use serde::{Deserialize, Serialize};

/// Controls storage challenge participation.
///
/// Disabling this stops rounds, proofs, attestations, and eviction while the
/// node continues sampling stored data. Disable it consistently across a fleet;
/// disabled nodes cannot be certified by their groups.
///
/// The two spans are wall clock, and `NodeConfig` turns each into the fewest
/// whole rounds that last it at the network's cadence, so a cadence change
/// leaves their meaning alone. Each omitted span takes the network preset's.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChallengeConfig {
    /// Whether to run challenge rounds and serve the challenge routes.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Fixed slot spacing, shared by all nodes. Omitted uses the network preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_slots: Option<u64>,
    /// Seconds allowed for fetching a spool after ownership changes. Omitted
    /// uses the network preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handover_grace_seconds: Option<u64>,
    /// Seconds of blank rounds in a row before the node re-reads protocol
    /// state. Omitted uses the network preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realign_after_blank_seconds: Option<u64>,
    /// Threads that verify incoming proofs, off the shared blocking pool.
    #[serde(default = "default_verify_workers")]
    pub verify_workers: usize,
    /// Log when a proof waits this long for verification. This is not a deadline.
    #[serde(default = "default_ingress_wait_budget_ms")]
    pub ingress_wait_budget_ms: u64,
}

impl Default for ChallengeConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            interval_slots: None,
            handover_grace_seconds: None,
            realign_after_blank_seconds: None,
            verify_workers: default_verify_workers(),
            ingress_wait_budget_ms: default_ingress_wait_budget_ms(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

/// One drains the queue it wakes on, so more threads only contend.
fn default_verify_workers() -> usize {
    1
}

/// Diagnostic queue latency threshold, independent of the round schedule.
fn default_ingress_wait_budget_ms() -> u64 {
    160
}
