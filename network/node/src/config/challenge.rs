use serde::Deserialize;

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

/// Sized to a slot, so it shrinks as the grid does.
fn default_ingress_wait_budget_ms() -> u64 {
    160
}
