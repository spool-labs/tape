use serde::Deserialize;

/// Controls eviction participation.
///
/// Disabling this preserves challenge records while preventing proposals and
/// votes. Challenge participation must also be enabled for eviction to run.
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
