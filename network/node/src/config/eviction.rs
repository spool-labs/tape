use serde::{Deserialize, Serialize};
use tape_core::challenge::EvictionPolicy;
use tape_core::types::BasisPoints;

/// Controls eviction participation.
///
/// Off unless turned on. While off, challenge records are still kept, and the
/// node makes no proposals and signs no votes. Challenge participation must
/// also be enabled for eviction to run.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvictionConfig {
    /// Whether to propose evictions and sign peers' eviction votes.
    #[serde(default)]
    pub enabled: bool,
    /// Evidence required before proposing an eviction, independent of epoch length.
    #[serde(default = "default_policy")]
    pub policy: EvictionPolicy,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            policy: default_policy(),
        }
    }
}

fn default_policy() -> EvictionPolicy {
    EvictionPolicy {
        max_consecutive_misses: 30,
        min_opportunities: 60,
        minimum_success_rate_bps: BasisPoints(5_000),
    }
}
