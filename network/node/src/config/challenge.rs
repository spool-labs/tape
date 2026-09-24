use serde::{Deserialize, Serialize};

/// Controls storage challenge participation.
///
/// Disabling this stops rounds, proofs, attestations, and eviction while the
/// node continues sampling stored data. Disable it consistently across a fleet;
/// disabled nodes cannot be certified by their groups.
///
/// The two spans are wall clock, and `NodeConfig` turns each into a round count
/// at the network's cadence, so a cadence change leaves their meaning alone.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChallengeConfig {
    /// Whether to run challenge rounds and serve the challenge routes.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Fixed slot spacing, shared by all nodes. Omitted uses the network preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_slots: Option<u64>,
    /// Seconds allowed for fetching a spool after ownership changes.
    #[serde(default = "default_handover_grace_seconds")]
    pub handover_grace_seconds: u64,
    /// Seconds of blank rounds in a row before the node re-reads protocol state.
    #[serde(default = "default_realign_after_blank_seconds")]
    pub realign_after_blank_seconds: u64,
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
            handover_grace_seconds: default_handover_grace_seconds(),
            realign_after_blank_seconds: default_realign_after_blank_seconds(),
            verify_workers: default_verify_workers(),
            ingress_wait_budget_ms: default_ingress_wait_budget_ms(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

/// Long enough to fetch a spool at fleet sizes, and the same span at every
/// cadence: the round count comes from the network's interval and slot time.
fn default_handover_grace_seconds() -> u64 {
    240
}

/// Recheck local state before a group-wide outage accumulates eviction evidence.
fn default_realign_after_blank_seconds() -> u64 {
    90
}

/// One drains the queue it wakes on, so more threads only contend.
fn default_verify_workers() -> usize {
    1
}

/// Diagnostic queue latency threshold, independent of the round schedule.
fn default_ingress_wait_budget_ms() -> u64 {
    160
}
