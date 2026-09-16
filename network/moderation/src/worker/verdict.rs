//! Deciding a check from the moderation API's results

use crate::types::category::Category;
use crate::worker::content::InputType;
use crate::worker::moderation::ModerationResponse;

/// What a check concluded
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict {
    Remove { categories: Vec<Category> },
    Clean,
    Unresolved { reason: String },
}

/// What was sent to the API
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SentInput {
    pub input_type: InputType,
    pub input_count: usize,
    /// The whole content was sent
    pub is_complete: bool,
}

/// Remove on any flagged removal category; clean only when provably so
pub fn decide(response: &ModerationResponse, sent: &SentInput, removal_categories: &[Category]) -> Verdict {
    let flagged = flagged_categories(response, removal_categories);
    if !flagged.is_empty() {
        return Verdict::Remove { categories: flagged };
    }
    match unresolved_reason(response, sent, removal_categories) {
        Some(reason) => Verdict::Unresolved { reason },
        None => Verdict::Clean,
    }
}

fn flagged_categories(response: &ModerationResponse, removal_categories: &[Category]) -> Vec<Category> {
    let mut flagged = Vec::new();
    for category in removal_categories {
        for result in &response.results {
            if result.flag(*category) == Some(true) && !flagged.contains(category) {
                flagged.push(*category);
            }
        }
    }
    flagged
}

fn unresolved_reason(
    response: &ModerationResponse,
    sent: &SentInput,
    removal_categories: &[Category],
) -> Option<String> {
    if !sent.is_complete {
        return Some("only part of the content was sent".to_string());
    }
    if response.results.len() != sent.input_count {
        return Some(format!(
            "{} results for {} inputs",
            response.results.len(),
            sent.input_count
        ));
    }
    for result in &response.results {
        for category in removal_categories {
            if result.flag(*category) != Some(false) {
                return Some(format!("{category} was not returned"));
            }
            if !result.applies_to(*category, sent.input_type) {
                return Some(format!("{category} does not apply to {} input", sent.input_type.as_str()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::worker::moderation::ModerationResult;

    const TEXT: SentInput = SentInput {
        input_type: InputType::Text,
        input_count: 1,
        is_complete: true,
    };

    fn result(flagged: &[Category], applied: &[&str]) -> ModerationResult {
        let mut categories = BTreeMap::new();
        let mut applied_types = BTreeMap::new();
        for category in Category::ALL {
            categories.insert(category.as_str().to_string(), Some(flagged.contains(&category)));
            let types = applied.iter().map(|name| name.to_string()).collect();
            applied_types.insert(category.as_str().to_string(), Some(types));
        }
        ModerationResult {
            flagged: Some(!flagged.is_empty()),
            categories: Some(categories),
            category_scores: None,
            category_applied_input_types: Some(applied_types),
        }
    }

    fn response(results: Vec<ModerationResult>) -> ModerationResponse {
        ModerationResponse {
            id: Some("modr-1".to_string()),
            model: Some("omni-moderation-latest".to_string()),
            results,
        }
    }

    // a flagged removal category removes even when content was cut short
    #[test]
    fn flagged() {
        let sent = SentInput {
            is_complete: false,
            ..TEXT
        };

        let verdict = decide(&response(vec![result(&[Category::Hate], &["text"])]), &sent, &Category::ALL);

        assert_eq!(verdict, Verdict::Remove { categories: vec![Category::Hate] });
    }

    // a flag outside the removal categories does not remove
    #[test]
    fn unconfigured() {
        let verdict = decide(&response(vec![result(&[Category::Hate], &["text"])]), &TEXT, &[Category::Sexual]);

        assert_eq!(verdict, Verdict::Clean);
    }

    // complete text with every category applied is clean
    #[test]
    fn clean() {
        let verdict = decide(&response(vec![result(&[], &["text"])]), &TEXT, &Category::ALL);

        assert_eq!(verdict, Verdict::Clean);
    }

    // partial content, missing values, missing results and unapplied categories stay unresolved
    #[test]
    fn unresolved() {
        let mut missing = result(&[], &["text"]);
        if let Some(categories) = missing.categories.as_mut() {
            categories.insert("hate".to_string(), None);
        }
        let partial = SentInput {
            is_complete: false,
            ..TEXT
        };
        let image = SentInput {
            input_type: InputType::Image,
            ..TEXT
        };

        let cases = [
            decide(&response(vec![result(&[], &["text"])]), &partial, &Category::ALL),
            decide(&response(vec![missing]), &TEXT, &Category::ALL),
            decide(&response(Vec::new()), &TEXT, &Category::ALL),
            decide(&response(vec![result(&[], &["text"])]), &image, &Category::ALL),
        ];

        for verdict in cases {
            assert!(matches!(verdict, Verdict::Unresolved { .. }), "{verdict:?}");
        }
    }
}
