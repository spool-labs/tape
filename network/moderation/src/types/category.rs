//! Categories the moderation API scores content under

use std::fmt;

use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};

/// One moderation category, named as the API returns it
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Category {
    Harassment,
    HarassmentThreatening,
    Hate,
    HateThreatening,
    Illicit,
    IllicitViolent,
    SelfHarm,
    SelfHarmInstructions,
    SelfHarmIntent,
    Sexual,
    SexualMinors,
    Violence,
    ViolenceGraphic,
}

impl Category {
    pub const ALL: [Category; 13] = [
        Self::Harassment,
        Self::HarassmentThreatening,
        Self::Hate,
        Self::HateThreatening,
        Self::Illicit,
        Self::IllicitViolent,
        Self::SelfHarm,
        Self::SelfHarmInstructions,
        Self::SelfHarmIntent,
        Self::Sexual,
        Self::SexualMinors,
        Self::Violence,
        Self::ViolenceGraphic,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Harassment => "harassment",
            Self::HarassmentThreatening => "harassment/threatening",
            Self::Hate => "hate",
            Self::HateThreatening => "hate/threatening",
            Self::Illicit => "illicit",
            Self::IllicitViolent => "illicit/violent",
            Self::SelfHarm => "self-harm",
            Self::SelfHarmInstructions => "self-harm/instructions",
            Self::SelfHarmIntent => "self-harm/intent",
            Self::Sexual => "sexual",
            Self::SexualMinors => "sexual/minors",
            Self::Violence => "violence",
            Self::ViolenceGraphic => "violence/graphic",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|category| category.as_str() == value)
    }
}

impl fmt::Display for Category {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Category {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value)
            .ok_or_else(|| DeserializeError::custom(format!("unknown moderation category `{value}`")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // every category name parses back to itself
    #[test]
    fn round_trip() {
        for category in Category::ALL {
            assert_eq!(Category::parse(category.as_str()), Some(category));
        }
        assert_eq!(Category::parse("sexual/adults"), None);
    }
}
