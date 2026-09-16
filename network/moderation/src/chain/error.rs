//! Chain reads and writes that failed

use std::path::PathBuf;

use tape_crypto::address::Address;
use thiserror::Error;

/// A chain operation or key that failed
#[derive(Debug, Error)]
pub enum ChainError {
    #[error("payer key {path}: {reason}")]
    PayerKey { path: PathBuf, reason: String },

    #[error("tape key {path}: {reason}")]
    TapeKey { path: PathBuf, reason: String },

    #[error("tape key {path} controls tape {actual}, not {expected}")]
    KeyMismatch {
        path: PathBuf,
        expected: Address,
        actual: Address,
    },

    #[error("no peer holds track {0}")]
    TrackNotFound(Address),

    #[error("solana rpc: {0}")]
    Rpc(String),

    #[error("tapedrive: {0}")]
    Sdk(String),
}
