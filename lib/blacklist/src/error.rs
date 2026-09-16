//! Tracks and subscriptions that cannot be used

use tape_crypto::address::Address;
use thiserror::Error;

/// A track or subscription that cannot be used
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BlacklistError {
    #[error("track is not utf-8 text")]
    NotText,

    #[error("line {line}: {reason}")]
    InvalidEntry { line: usize, reason: String },

    #[error("an entry does not fit a track of {max_bytes} bytes")]
    TrackLimit { max_bytes: usize },

    #[error("subscription `{0}` is listed more than once")]
    DuplicateName(String),

    #[error("tape {0} is subscribed more than once")]
    DuplicateTape(Address),

    #[error("subscription `{0}` honors no actions")]
    NoActions(String),
}
