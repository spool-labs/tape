//! The reason a reporter gives

/// A report's reason, as the report API accepts it
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Reason {
    Sexual,
    Hate,
    Violence,
    SelfHarm,
    Other,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sexual => "sexual",
            Self::Hate => "hate",
            Self::Violence => "violence",
            Self::SelfHarm => "self-harm",
            Self::Other => "other",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sexual" => Some(Self::Sexual),
            "hate" => Some(Self::Hate),
            "violence" => Some(Self::Violence),
            "self-harm" => Some(Self::SelfHarm),
            "other" => Some(Self::Other),
            _ => None,
        }
    }
}
