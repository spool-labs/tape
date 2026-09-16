//! When removals on a tape unlist the whole tape

use crate::config::settings::TapeRuleConfig;
use crate::types::category::Category;
use crate::types::ids::RemovalCount;

/// Unlist on a triggering category or once removals reach a count
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TapeRule {
    pub triggering_categories: Vec<Category>,
    pub removal_count: RemovalCount,
}

impl TapeRule {
    pub fn new(config: &TapeRuleConfig) -> Self {
        Self {
            triggering_categories: config.triggering_categories.clone(),
            removal_count: config.removal_count,
        }
    }

    /// Whether a removal flagged under `categories`, bringing the tape to
    /// `removals`, unlists the tape
    pub fn should_unlist(&self, categories: &[Category], removals: RemovalCount) -> bool {
        let is_triggered = categories
            .iter()
            .any(|category| self.triggering_categories.contains(category));
        is_triggered || removals >= self.removal_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> TapeRule {
        TapeRule::new(&TapeRuleConfig::default())
    }

    // one removal in a triggering category unlists
    #[test]
    fn triggering() {
        assert!(rule().should_unlist(&[Category::Hate, Category::SexualMinors], RemovalCount(1)));
    }

    // other categories unlist only once removals reach the count
    #[test]
    fn count() {
        assert!(!rule().should_unlist(&[Category::Hate], RemovalCount(2)));
        assert!(rule().should_unlist(&[Category::Hate], RemovalCount(3)));
    }

    // both thresholds follow the config
    #[test]
    fn configured() {
        let rule = TapeRule::new(&TapeRuleConfig {
            triggering_categories: vec![Category::Violence],
            removal_count: RemovalCount(1),
        });

        assert!(rule.should_unlist(&[Category::Violence], RemovalCount(0)));
        assert!(!rule.should_unlist(&[Category::SexualMinors], RemovalCount(0)));
        assert!(rule.should_unlist(&[], RemovalCount(1)));
    }
}
