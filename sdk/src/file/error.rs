//! Unified error type for file-level SDK operations

use thiserror::Error;

/// Errors raised while encoding, validating, or assembling files
#[derive(Debug, Error)]
pub enum FileError {
    #[error("invalid file input: {0}")]
    InvalidInput(&'static str),

    #[error("tape has {available} track slots remaining, but file needs {needed} ({chunks} chunks + 1 manifest)")]
    InsufficientTrackSlots {
        available: u64,
        needed: u64,
        chunks: u64,
    },

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("chunk error: {0}")]
    Chunk(String),

    #[error("file integrity error: {0}")]
    Integrity(String),
}
