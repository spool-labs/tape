//! Whether this node runs the storage challenge.

use serde::Deserialize;

/// Challenge participation
///
/// Off, the node opens no round, answers none, and takes in no peer's proof or
/// attestation. It keeps sampling what it stores, so turning it back on needs
/// no backfill. A node that answers nothing cannot be certified by its group,
/// so this belongs on a fleet that has the challenge off everywhere.
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
