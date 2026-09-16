//! Rack tracks and records that cannot be used

use thiserror::Error;

/// A rack track or record that cannot be used
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RackError {
    #[error("rack track is not utf-8 text")]
    NotText,

    #[error("line {line}: {reason}")]
    InvalidRecord { line: usize, reason: String },

    #[error("metadata key `{0}` is not 1 to 32 of a-z 0-9 _ . -")]
    InvalidKey(String),

    #[error("metadata key `{0}` is used more than once")]
    DuplicateKey(String),

    #[error("a record does not fit a rack track of {max_bytes} bytes")]
    RecordLimit { max_bytes: usize },
}
