//! Whether this node runs the storage challenge.

use serde::Deserialize;

/// Challenge participation.
///
/// Off, the node opens no round, answers no challenge and takes in no peer's
/// proof or attestation. It still samples what it stores, so turning the
/// challenge back on needs no backfill.
///
/// A node that answers nothing is a node its group cannot certify. Turn this
/// off only where the rest of the fleet has it off too, or the group reads the
/// silence as a miss and evicts.
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
