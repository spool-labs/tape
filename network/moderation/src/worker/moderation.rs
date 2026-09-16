//! The moderation API client and its response

use std::collections::{BTreeMap, BTreeSet};

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::config::settings::ModerationConfig;
use crate::records::rows::CategoryScore;
use crate::types::category::Category;
use crate::worker::body::read_bounded;
use crate::worker::content::InputType;
use crate::worker::error::WorkerError;

const MODERATIONS_PATH: &str = "/moderations";
const JSON_CONTENT: &str = "application/json";
const IMAGE_PART_TYPE: &str = "image_url";

/// Sends content to `POST {base_url}/moderations`
pub struct ModerationClient {
    client: Client,
    url: String,
    model: String,
    api_key: String,
    max_response_bytes: usize,
}

/// The `input` of a moderation request
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ModerationInput {
    /// One result per string
    Texts(Vec<String>),
    Images(Vec<ImagePart>),
}

#[derive(Clone, Debug, Serialize)]
pub struct ImagePart {
    #[serde(rename = "type")]
    pub part_type: String,
    pub image_url: ImageUrl,
}

#[derive(Clone, Debug, Serialize)]
pub struct ImageUrl {
    pub url: String,
}

#[derive(Serialize)]
struct ModerationRequest {
    model: String,
    input: ModerationInput,
}

/// A moderation response; any value may be missing or null
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ModerationResponse {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub results: Vec<ModerationResult>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ModerationResult {
    #[serde(default)]
    pub flagged: Option<bool>,
    #[serde(default)]
    pub categories: Option<BTreeMap<String, Option<bool>>>,
    #[serde(default)]
    pub category_scores: Option<BTreeMap<String, Option<f64>>>,
    #[serde(default)]
    pub category_applied_input_types: Option<BTreeMap<String, Option<Vec<String>>>>,
}

impl ModerationClient {
    pub fn try_new(config: &ModerationConfig, api_key: String) -> Result<Self, WorkerError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| WorkerError::Client(error.to_string()))?;
        Ok(Self {
            client,
            url: format!("{}{MODERATIONS_PATH}", config.base_url.trim_end_matches('/')),
            model: config.model.clone(),
            api_key,
            max_response_bytes: config.max_response_bytes,
        })
    }

    pub async fn moderate(&self, input: ModerationInput) -> Result<ModerationResponse, WorkerError> {
        let request = ModerationRequest {
            model: self.model.clone(),
            input,
        };
        let body = serde_json::to_vec(&request).map_err(|error| WorkerError::ModerationDecode(error.to_string()))?;
        let response = self
            .client
            .post(&self.url)
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .header(CONTENT_TYPE, JSON_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(WorkerError::ModerationRequest)?;

        let status = response.status();
        if !status.is_success() {
            return Err(WorkerError::ModerationStatus(status.as_u16()));
        }
        let body = read_bounded(response, self.max_response_bytes)
            .await
            .map_err(WorkerError::ModerationRequest)?;
        if body.is_truncated {
            return Err(WorkerError::ModerationTooLarge(self.max_response_bytes));
        }
        serde_json::from_slice(&body.bytes).map_err(|error| WorkerError::ModerationDecode(error.to_string()))
    }
}

impl ModerationInput {
    pub fn image(data_url: String) -> Self {
        Self::Images(vec![ImagePart {
            part_type: IMAGE_PART_TYPE.to_string(),
            image_url: ImageUrl { url: data_url },
        }])
    }

    pub fn input_count(&self) -> usize {
        match self {
            Self::Texts(texts) => texts.len(),
            Self::Images(images) => images.len(),
        }
    }
}

impl ModerationResponse {
    /// Every category of every result, as returned
    pub fn category_scores(&self) -> Vec<CategoryScore> {
        let mut scores = Vec::new();
        for (index, result) in self.results.iter().enumerate() {
            let result_index = u32::try_from(index).unwrap_or(u32::MAX);
            for name in result.category_names() {
                scores.push(CategoryScore {
                    result_index,
                    flagged: result.value(&name),
                    score: result.score(&name),
                    applied_input_types: result.applied_input_types(&name),
                    category: name,
                });
            }
        }
        scores
    }
}

impl ModerationResult {
    /// The flag for a category: `None` when missing or null
    pub fn flag(&self, category: Category) -> Option<bool> {
        self.value(category.as_str())
    }

    /// Whether the API applied a category to this kind of input
    pub fn applies_to(&self, category: Category, input_type: InputType) -> bool {
        self.applied_input_types(category.as_str())
            .is_some_and(|types| types.iter().any(|name| name == input_type.as_str()))
    }

    fn value(&self, name: &str) -> Option<bool> {
        self.categories
            .as_ref()
            .and_then(|categories| categories.get(name).copied().flatten())
    }

    fn score(&self, name: &str) -> Option<f64> {
        self.category_scores
            .as_ref()
            .and_then(|scores| scores.get(name).copied().flatten())
    }

    fn applied_input_types(&self, name: &str) -> Option<Vec<String>> {
        self.category_applied_input_types
            .as_ref()
            .and_then(|applied| applied.get(name).cloned().flatten())
    }

    fn category_names(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        if let Some(categories) = &self.categories {
            names.extend(categories.keys().cloned());
        }
        if let Some(scores) = &self.category_scores {
            names.extend(scores.keys().cloned());
        }
        if let Some(applied) = &self.category_applied_input_types {
            names.extend(applied.keys().cloned());
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // null values parse and are kept as null, never as false or zero
    #[test]
    fn null_values() {
        let body = r#"{
            "id": "modr-1",
            "model": "omni-moderation-latest",
            "results": [{
                "flagged": false,
                "categories": {"hate": null, "sexual": false},
                "category_scores": {"hate": null, "sexual": 0.01},
                "category_applied_input_types": {"hate": null, "sexual": ["text"]}
            }]
        }"#;

        let response: ModerationResponse = serde_json::from_str(body).expect("parse");

        let result = &response.results[0];
        assert_eq!(result.flag(Category::Hate), None);
        assert_eq!(result.flag(Category::Sexual), Some(false));
        assert!(!result.applies_to(Category::Hate, InputType::Text));
        let scores = response.category_scores();
        assert_eq!(scores.len(), 2);
        assert_eq!(scores[0].category, "hate");
        assert_eq!(scores[0].score, None);
        assert_eq!(scores[1].applied_input_types, Some(vec!["text".to_string()]));
    }

    // texts serialize as strings and images as typed parts
    #[test]
    fn request_shape() {
        let texts = serde_json::to_value(ModerationInput::Texts(vec!["hello".to_string()])).expect("texts");
        let image = serde_json::to_value(ModerationInput::image("data:image/png;base64,AA".to_string())).expect("image");

        assert_eq!(texts, serde_json::json!(["hello"]));
        assert_eq!(
            image,
            serde_json::json!([{"type": "image_url", "image_url": {"url": "data:image/png;base64,AA"}}])
        );
    }
}
