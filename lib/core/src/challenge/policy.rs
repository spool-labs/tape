//! Explicit rules for judging challenge history.
//!
//! A record contains observations, not policy. Callers supply the rules for
//! their network; neither epoch duration nor process initialization selects them.

use serde::{Deserialize, Serialize};

use crate::types::BasisPoints;

/// Evidence required before a peer's challenge record warrants eviction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvictionPolicy {
    /// Consecutive judged misses that trigger the reachability rule.
    pub max_consecutive_misses: u64,
    /// Judged rounds required before evaluating the lifetime success rate.
    pub min_opportunities: u64,
    /// A lifetime success rate strictly below this floor triggers the rate rule.
    pub minimum_success_rate_bps: BasisPoints,
}

impl EvictionPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_consecutive_misses == 0 {
            return Err("max_consecutive_misses must be greater than zero");
        }
        if self.min_opportunities == 0 {
            return Err("min_opportunities must be greater than zero");
        }
        if !self.minimum_success_rate_bps.is_valid() {
            return Err("minimum_success_rate_bps must be between 0 and 10000");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_rules_that_cannot_judge_evidence() {
        let valid = EvictionPolicy {
            max_consecutive_misses: 30,
            min_opportunities: 60,
            minimum_success_rate_bps: BasisPoints(5_000),
        };
        assert!(valid.validate().is_ok());
        assert!(
            EvictionPolicy {
                max_consecutive_misses: 0,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            EvictionPolicy {
                min_opportunities: 0,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            EvictionPolicy {
                minimum_success_rate_bps: BasisPoints(10_001),
                ..valid
            }
            .validate()
            .is_err()
        );
    }
}
