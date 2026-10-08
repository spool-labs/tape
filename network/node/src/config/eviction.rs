use serde::{Deserialize, Serialize};

/// Controls eviction participation.
///
/// Off unless turned on. While off, the node makes no proposals and signs no
/// votes.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvictionConfig {
    /// Whether to propose evictions and sign peers' eviction votes.
    #[serde(default)]
    pub enabled: bool,
}
