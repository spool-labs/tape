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
}

impl Default for ChallengeConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
