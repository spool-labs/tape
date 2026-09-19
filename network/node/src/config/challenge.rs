use serde::{Deserialize, Serialize};

/// Controls storage challenge participation.
///
/// Disabling this stops rounds, proofs, attestations, and eviction while the
/// node continues sampling stored data. Disable it consistently across a fleet;
/// disabled nodes cannot be certified by their groups.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChallengeConfig {
    /// Whether to run challenge rounds and serve the challenge routes.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Fixed slot spacing, shared by all nodes. Omitted uses the network preset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_slots: Option<u64>,
    /// Rounds allowed for fetching a spool after ownership changes.
    #[serde(default = "default_handover_grace_rounds")]
    pub handover_grace_rounds: u64,
    /// Blank rounds in a row before the node re-reads protocol state.
    #[serde(default = "default_realign_after_blank_rounds")]
    pub realign_after_blank_rounds: u64,
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
            handover_grace_rounds: default_handover_grace_rounds(),
            realign_after_blank_rounds: default_realign_after_blank_rounds(),
            verify_workers: default_verify_workers(),
            ingress_wait_budget_ms: default_ingress_wait_budget_ms(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

fn default_handover_grace_rounds() -> u64 {
    8
}

/// Recheck local state before a group-wide outage accumulates eviction evidence.
fn default_realign_after_blank_rounds() -> u64 {
    3
}

/// One drains the queue it wakes on, so more threads only contend.
fn default_verify_workers() -> usize {
    1
}

/// Diagnostic queue latency threshold, independent of the round schedule.
fn default_ingress_wait_budget_ms() -> u64 {
    160
}
