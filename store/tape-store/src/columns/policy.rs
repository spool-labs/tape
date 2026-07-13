//! Write-authorization policy ruleset column family.

use store::Column;

use crate::types::{PolicyRule, PolicyRuleKey};

/// Ordered write-authorization policy rules.
pub struct PolicyRuleCol;

impl Column for PolicyRuleCol {
    const CF_NAME: &'static str = "policy_rule";
    type Key = PolicyRuleKey;
    type Value = PolicyRule;
}
