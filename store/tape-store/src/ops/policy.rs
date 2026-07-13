//! Write-authorization policy-engine operations

use store::Store;
use tape_crypto::address::Address;

use crate::columns::PolicyRuleCol;
use crate::error::Result;
use crate::types::{PolicyAction, PolicyEffect, PolicyRule, PolicyRuleKey};
use crate::TapeStore;

/// The outcome of a policy evaluation:.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyDecision {
    /// Whether the write is permitted
    pub is_allowed: bool,
    /// Operator-facing reason code for the decision.
    pub reason: String,
}

/// Operations for the durable write-authorization policy engine
pub trait PolicyOps {
    /// Insert or overwrite the policy rule at `key`
    fn put_policy_rule(&self, key: PolicyRuleKey, rule: &PolicyRule) -> Result<()>;

    /// Delete the policy rule at `key`
    ///
    /// Returns `true` when a rule existed.
    fn delete_policy_rule(&self, key: &PolicyRuleKey) -> Result<bool>;

    /// List every policy rule as `(key, rule)`, in priority order
    fn list_policy_rules(&self) -> Result<Vec<(PolicyRuleKey, PolicyRule)>>;

    /// Evaluate the ruleset for a concrete `(principal, bucket, action)` request.
    fn evaluate_policy(
        &self,
        principal: &Address,
        bucket: &Address,
        action: PolicyAction,
        is_default_allow: bool,
    ) -> Result<PolicyDecision>;
}

impl<Backend: Store> PolicyOps for TapeStore<Backend> {
    fn put_policy_rule(&self, key: PolicyRuleKey, rule: &PolicyRule) -> Result<()> {
        self.put::<PolicyRuleCol>(&key, rule)?;
        Ok(())
    }

    fn delete_policy_rule(&self, key: &PolicyRuleKey) -> Result<bool> {
        if self.contains::<PolicyRuleCol>(key)? {
            self.delete::<PolicyRuleCol>(key)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_policy_rules(&self) -> Result<Vec<(PolicyRuleKey, PolicyRule)>> {
        Ok(self.iter::<PolicyRuleCol>()?)
    }

    fn evaluate_policy(
        &self,
        principal: &Address,
        bucket: &Address,
        action: PolicyAction,
        is_default_allow: bool,
    ) -> Result<PolicyDecision> {
        let mut allow_reason: Option<String> = None;
        for (_key, rule) in self.iter::<PolicyRuleCol>()? {
            if !rule.matches(principal, bucket, action) {
                continue;
            }
            match rule.effect {
                PolicyEffect::Deny => {
                    return Ok(PolicyDecision {
                        is_allowed: false,
                        reason: rule.reason,
                    });
                }
                PolicyEffect::Allow => {
                    if allow_reason.is_none() {
                        allow_reason = Some(rule.reason);
                    }
                }
            }
        }

        if let Some(reason) = allow_reason {
            return Ok(PolicyDecision {
                is_allowed: true,
                reason,
            });
        }

        Ok(if is_default_allow {
            PolicyDecision {
                is_allowed: true,
                reason: "default-allow".to_string(),
            }
        } else {
            PolicyDecision {
                is_allowed: false,
                reason: "default-deny".to_string(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn rule(
        principal: Option<Address>,
        bucket: Option<Address>,
        action: PolicyAction,
        effect: PolicyEffect,
        reason: &str,
    ) -> PolicyRule {
        PolicyRule {
            principal,
            bucket,
            action,
            effect,
            reason: reason.to_string(),
        }
    }

    // an empty ruleset honors the caller default
    #[test]
    fn empty_ruleset() {
        let s = store();
        let p = Address::new_unique();
        let b = Address::new_unique();

        let deny = s
            .evaluate_policy(&p, &b, PolicyAction::Put, false)
            .expect("evaluate policy");
        assert!(!deny.is_allowed);
        assert_eq!(deny.reason, "default-deny");

        let allow = s
            .evaluate_policy(&p, &b, PolicyAction::Put, true)
            .expect("evaluate policy");
        assert!(allow.is_allowed);
        assert_eq!(allow.reason, "default-allow");
    }

    // an allow rule permits a matching request
    #[test]
    fn allow_rule() {
        let s = store();
        let p = Address::new_unique();
        let b = Address::new_unique();
        s.put_policy_rule(
            PolicyRuleKey::new(10, 1),
            &rule(Some(p), Some(b), PolicyAction::Any, PolicyEffect::Allow, "owner ok"),
        )
        .expect("put rule");

        let decision = s
            .evaluate_policy(&p, &b, PolicyAction::Put, false)
            .expect("evaluate policy");
        assert!(decision.is_allowed);
        assert_eq!(decision.reason, "owner ok");

        // A different principal is not matched, so the default decides.
        let other = s
            .evaluate_policy(&Address::new_unique(), &b, PolicyAction::Put, false)
            .expect("evaluate policy");
        assert!(!other.is_allowed);
    }

    // deny wins over allow regardless of rule order
    #[test]
    fn deny_precedence() {
        let s = store();
        let p = Address::new_unique();
        let b = Address::new_unique();
        // Allow at a lower priority, deny at a higher priority — and vice versa:
        // either way the deny must win.
        s.put_policy_rule(
            PolicyRuleKey::new(1, 1),
            &rule(None, None, PolicyAction::Any, PolicyEffect::Allow, "broad allow"),
        )
        .expect("put rule");
        s.put_policy_rule(
            PolicyRuleKey::new(99, 2),
            &rule(Some(p), Some(b), PolicyAction::Delete, PolicyEffect::Deny, "no deletes"),
        )
        .expect("put rule");

        let decision = s
            .evaluate_policy(&p, &b, PolicyAction::Delete, true)
            .expect("evaluate policy");
        assert!(!decision.is_allowed, "deny must win over allow");
        assert_eq!(decision.reason, "no deletes");

        // A Put on the same subject is only matched by the broad allow.
        let put = s
            .evaluate_policy(&p, &b, PolicyAction::Put, false)
            .expect("evaluate policy");
        assert!(put.is_allowed);
        assert_eq!(put.reason, "broad allow");
    }

    // a rule can be added, listed, and deleted
    #[test]
    fn delete_list() {
        let s = store();
        let key = PolicyRuleKey::new(5, 7);
        assert!(!s.delete_policy_rule(&key).expect("delete rule"));
        s.put_policy_rule(
            key,
            &rule(None, None, PolicyAction::Any, PolicyEffect::Deny, "x"),
        )
        .expect("put rule");
        assert_eq!(s.list_policy_rules().expect("list rules").len(), 1);
        assert!(s.delete_policy_rule(&key).expect("delete rule"));
        assert!(s.list_policy_rules().expect("list rules").is_empty());
    }
}
