//! Error types for tape-store operations

use crate::types::Pubkey;
use thiserror::Error;

/// Errors that can occur during tape-store operations
#[derive(Debug, Error)]
pub enum TapeStoreError {
    /// Underlying store error
    #[error("Store error: {0}")]
    Store(#[from] store::Error),

    /// Track not found
    #[error("Track not found: {0:?}")]
    TrackNotFound(Pubkey),

    /// Slice not found
    #[error("Slice not found: spool={0}, track={1:?}")]
    SliceNotFound(u16, Pubkey),

    /// Spool not found
    #[error("Spool not found: {0}")]
    SpoolNotFound(u16),

    /// Committee not found for epoch
    #[error("Committee not found for epoch {0}")]
    CommitteeNotFound(u64),

    /// Recovery entry not found
    #[error("Recovery entry not found: spool={0}, track={1:?}")]
    RecoveryNotFound(u16, Pubkey),

    /// Invalid slice count
    #[error("Invalid slice count: expected 1024, got {0}")]
    InvalidSliceCount(usize),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for tape-store operations
pub type Result<T> = std::result::Result<T, TapeStoreError>;
