//! Error types for tape-store operations

use crate::types::Pubkey;
use tape_core::types::EpochNumber;
use thiserror::Error;

/// Errors that can occur during tape-store operations
#[derive(Debug, Error)]
pub enum TapeStoreError {
    /// Underlying store error
    #[error("Store error: {0}")]
    Store(#[from] store::Error),

    /// Tape info not found
    #[error("Tape info not found: {0:?}")]
    TapeNotFound(Pubkey),

    /// Track info not found
    #[error("Track info not found: {0:?}")]
    TrackNotFound(Pubkey),

    /// Slice not found
    #[error("Slice not found: spool={0}, track={1:?}")]
    SliceNotFound(u16, Pubkey),

    /// Spool not found
    #[error("Spool not found: spool={0}")]
    SpoolNotFound(u16),

    /// Committee not found for epoch
    #[error("Committee not found for epoch {0}")]
    CommitteeNotFound(EpochNumber),

    /// Invalid data length
    #[error("Invalid data length: expected {expected}, got {actual}")]
    InvalidDataLength { expected: usize, actual: usize },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for tape-store operations
pub type Result<T> = std::result::Result<T, TapeStoreError>;
