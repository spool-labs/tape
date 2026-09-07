use serde::Deserialize;
use tape_core::challenge::record::MAX_CONSECUTIVE_MISSES;

/// Controls storage challenge participation.
///
/// Disabling this stops rounds, proofs, attestations, and eviction while the
/// node continues sampling stored data. Disable it consistently across a fleet;
/// disabled nodes cannot be certified by their groups.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ChallengeConfig {
    /// Whether to run challenge rounds and serve the challenge routes.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Blank rounds in a row before the node re-reads protocol state.
    #[serde(default = "default_realign_after_blank_rounds")]
    pub realign_after_blank_rounds: u64,
    /// Threads that verify incoming proofs, off the shared blocking pool.
    #[serde(default = "default_verify_workers")]
    pub verify_workers: usize,
    /// Wait a proof may spend reaching a verify thread before it has missed its slot.
    #[serde(default = "default_ingress_wait_budget_ms")]
    pub ingress_wait_budget_ms: u64,
}

impl Default for ChallengeConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            realign_after_blank_rounds: default_realign_after_blank_rounds(),
            verify_workers: default_verify_workers(),
            ingress_wait_budget_ms: default_ingress_wait_budget_ms(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

/// `MAX_CONSECUTIVE_MISSES` rounds, so the tripwire fires on the same round the
/// run arm would first condemn a peer rather than seven rounds after it.
fn default_realign_after_blank_rounds() -> u64 {
    MAX_CONSECUTIVE_MISSES
}

/// One drains the queue it wakes on, so more threads only contend.
fn default_verify_workers() -> usize {
    1
}

/// Sized to a slot, so it shrinks as the grid does.
fn default_ingress_wait_budget_ms() -> u64 {
    160
}
