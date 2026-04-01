//! Unified error type for stream-level SDK operations.

use thiserror::Error;
use tape_core::types::TrackNumber;

/// Errors raised while encoding, validating, or assembling byte streams.
#[derive(Debug, Error)]
pub enum StreamError {
    #[error("invalid stream input: {0}")]
    InvalidInput(String),

    #[error("tape has {available} track slots remaining, but stream needs {needed} ({chunks} chunks + 1 manifest)")]
    InsufficientTrackSlots {
        available: TrackNumber,
        needed: TrackNumber,
        chunks: TrackNumber,
    },

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("chunk error: {0}")]
    Chunk(String),

    #[error("stream integrity error: {0}")]
    Integrity(String),
}
