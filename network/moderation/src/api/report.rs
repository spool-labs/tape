//! The report request body and its validation

use std::str::FromStr;

use serde::Deserialize;
use tape_crypto::address::Address;

use crate::api::error::ApiError;
use crate::types::reason::Reason;
use crate::types::target::{Target, TargetKind};

pub const NOTE_MAX_CHARACTERS: usize = 1_000;
pub const KEY_MIN_CHARACTERS: usize = 16;
pub const KEY_MAX_CHARACTERS: usize = 128;

#[derive(Deserialize)]
struct ReportBody {
    target: TargetBody,
    reason: String,
    #[serde(default)]
    note: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
struct TargetBody {
    kind: String,
    address: String,
}

/// A report that follows the contract
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidReport {
    pub target: Target,
    pub reason: Reason,
    pub note: Option<String>,
    pub idempotency_key: String,
}

pub fn parse_report(body: &[u8]) -> Result<ValidReport, ApiError> {
    let body: ReportBody = serde_json::from_slice(body).map_err(|error| ApiError::Body(error.to_string()))?;
    let kind = TargetKind::parse(&body.target.kind).ok_or_else(|| ApiError::Kind(body.target.kind.clone()))?;
    let address = Address::from_str(&body.target.address).map_err(|_| ApiError::Address(body.target.address.clone()))?;
    let reason = Reason::parse(&body.reason).ok_or_else(|| ApiError::Reason(body.reason.clone()))?;
    let note = normalize_note(body.note)?;
    if !is_valid_key(&body.idempotency_key) {
        return Err(ApiError::IdempotencyKey);
    }

    Ok(ValidReport {
        target: Target { kind, address },
        reason,
        note,
        idempotency_key: body.idempotency_key,
    })
}

/// Trimmed; blank counts as absent
fn normalize_note(note: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(note) = note else {
        return Ok(None);
    };
    let trimmed = note.trim();
    match (trimmed.is_empty(), trimmed.chars().count() > NOTE_MAX_CHARACTERS) {
        (true, _) => Ok(None),
        (false, true) => Err(ApiError::NoteLength),
        (false, false) => Ok(Some(trimmed.to_string())),
    }
}

fn is_valid_key(key: &str) -> bool {
    let is_valid_length = (KEY_MIN_CHARACTERS..=KEY_MAX_CHARACTERS).contains(&key.len());
    is_valid_length
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '-')
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const ADDRESS: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    fn body(value: serde_json::Value) -> Vec<u8> {
        value.to_string().into_bytes()
    }

    fn valid() -> serde_json::Value {
        json!({
            "target": {"kind": "track", "address": ADDRESS},
            "reason": "self-harm",
            "note": "  some context  ",
            "idempotency_key": "4f1c9a0e7b2d4e6f8a1b3c5d7e9f0a2b",
        })
    }

    // the contract's example parses, with the note trimmed
    #[test]
    fn example() {
        let report = parse_report(&body(valid())).expect("parse");

        assert_eq!(report.target.kind, TargetKind::Track);
        assert_eq!(report.target.address.to_string(), ADDRESS);
        assert_eq!(report.reason, Reason::SelfHarm);
        assert_eq!(report.note.as_deref(), Some("some context"));
    }

    // a blank or missing note is absent, and a tape target is accepted
    #[test]
    fn optional_note() {
        let mut blank = valid();
        blank["note"] = json!("   ");
        blank["target"]["kind"] = json!("tape");
        let mut missing = valid();
        if let Some(fields) = missing.as_object_mut() {
            fields.remove("note");
        }

        let blank = parse_report(&body(blank)).expect("blank note");
        let missing = parse_report(&body(missing)).expect("missing note");

        assert_eq!(blank.note, None);
        assert_eq!(blank.target.kind, TargetKind::Tape);
        assert_eq!(missing.note, None);
    }

    // each broken field is refused
    #[test]
    fn malformed() {
        let mut cases = Vec::new();
        for (pointer, value) in [
            ("/target/kind", json!("object")),
            ("/target/address", json!("not-an-address")),
            ("/reason", json!("spam")),
            ("/note", json!("x".repeat(NOTE_MAX_CHARACTERS + 1))),
            ("/idempotency_key", json!("short")),
            ("/idempotency_key", json!("has spaces in the idempotency key")),
            ("/idempotency_key", json!("k".repeat(KEY_MAX_CHARACTERS + 1))),
        ] {
            let mut report = valid();
            if let Some(field) = report.pointer_mut(pointer) {
                *field = value;
            }
            cases.push(body(report));
        }
        cases.push(b"not json".to_vec());
        cases.push(body(json!({"target": {"kind": "track"}})));

        for case in cases {
            assert!(parse_report(&case).is_err(), "{}", String::from_utf8_lossy(&case));
        }
    }
}
