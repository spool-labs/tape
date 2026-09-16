//! Durations written as text, like `500ms`, `10s`, `5m` or `1h`

use std::time::Duration;

use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 3_600;

pub fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let digits = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(digits);
    let amount: u64 = number
        .parse()
        .map_err(|_| format!("`{value}` is not a duration like 10s, 5m or 1h"))?;

    let duration = match unit.trim() {
        "ms" => Duration::from_millis(amount),
        "s" => Duration::from_secs(amount),
        "m" => Duration::from_secs(amount.saturating_mul(SECONDS_PER_MINUTE)),
        "h" => Duration::from_secs(amount.saturating_mul(SECONDS_PER_HOUR)),
        other => return Err(format!("unknown unit `{other}` in duration `{value}`")),
    };
    Ok(duration)
}

/// Deserialize a text duration
pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_duration(&value).map_err(DeserializeError::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    // each unit scales the number
    #[test]
    fn units() {
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7_200)));
    }

    // a missing number or unknown unit is refused
    #[test]
    fn invalid() {
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("10 days").is_err());
    }
}
