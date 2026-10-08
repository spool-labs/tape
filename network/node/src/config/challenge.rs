use serde::{Deserialize, Serialize};

/// Controls challenge rounds.
///
/// Off unless turned on. While off, the node sends no proofs, refuses its
/// peers' proofs and keeps no board.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChallengeConfig {
    /// Whether to prove this node's spools to its group peers and record theirs.
    #[serde(default)]
    pub enabled: bool,
}
