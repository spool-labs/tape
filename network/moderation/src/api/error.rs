//! Report requests and API setup that cannot be used

use thiserror::Error;

use crate::api::report::{KEY_MAX_CHARACTERS, KEY_MIN_CHARACTERS, NOTE_MAX_CHARACTERS};

/// A malformed report, answered with 400, or a router that cannot be built
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("body is not a report: {0}")]
    Body(String),

    #[error("unknown target kind `{0}`")]
    Kind(String),

    #[error("`{0}` is not a base58 address")]
    Address(String),

    #[error("unknown reason `{0}`")]
    Reason(String),

    #[error("note is longer than {NOTE_MAX_CHARACTERS} characters")]
    NoteLength,

    #[error("idempotency_key must be {KEY_MIN_CHARACTERS} to {KEY_MAX_CHARACTERS} characters of A-Z a-z 0-9 _ -")]
    IdempotencyKey,

    #[error("allowed origin `{0}` is not a valid header value")]
    Origin(String),
}
