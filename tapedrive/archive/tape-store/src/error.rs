//! Error types for tape-store operations

use crate::types::{TapeNumber, TrackNumber};
use thiserror::Error;

/// Errors that can occur during tape-store operations
#[derive(Debug, Error)]
pub enum TapeStoreError {
    /// Underlying store error
    #[error("Store error: {0}")]
    Store(#[from] store::Error),

    /// Tape not found
    #[error("Tape not found: {0:?}")]
    TapeNotFound(TapeNumber),

    /// Track not found
    #[error("Track not found: {0:?}")]
    TrackNotFound(TrackNumber),

    /// Slice not found
    #[error("Slice not found: track={0:?}, spool={1}")]
    SliceNotFound(TrackNumber, u16),

    /// Inconsistent index: tape has address index but no tape data
    #[error("Inconsistent tape index: tape {0:?} has address index but no tape data")]
    InconsistentTapeIndex(TapeNumber),

    /// Inconsistent index: track has address index but no track data
    #[error("Inconsistent track index: track {0:?} has address index but no track data")]
    InconsistentTrackIndex(TrackNumber),

    /// Invalid slice count
    #[error("Invalid slice count: expected 1024, got {0}")]
    InvalidSliceCount(usize),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for tape-store operations
pub type Result<T> = std::result::Result<T, TapeStoreError>;
