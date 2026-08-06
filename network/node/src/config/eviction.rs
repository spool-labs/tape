//! Whether this node takes part in evicting its peers.

use serde::Deserialize;

/// Eviction participation.
///
/// Off, the node keeps its challenge records and still shows them, but proposes
/// nothing and signs no peer's proposal. Records are one node's own observation
/// either way, so an operator that does not want to act on them can say so
/// without going dark on the mechanism.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct EvictionConfig {
    /// Whether to propose evictions and sign peers' eviction votes.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
        }
    }
}

fn default_enabled() -> bool {
    true
}
