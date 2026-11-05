//! Errors of the spool feature

use tape_core::encoding::ProfileError;
use tape_slicer::{DecodeError, EncodeError};
use tape_store::error::TapeStoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpoolError {
    #[error("store error: {0}")]
    Store(#[from] TapeStoreError),
    #[error("slicer error: {0}")]
    Slicer(String),
}

impl From<ProfileError> for SpoolError {
    fn from(error: ProfileError) -> Self {
        Self::Slicer(error.to_string())
    }
}

impl From<DecodeError> for SpoolError {
    fn from(error: DecodeError) -> Self {
        Self::Slicer(error.to_string())
    }
}

impl From<EncodeError> for SpoolError {
    fn from(error: EncodeError) -> Self {
        Self::Slicer(error.to_string())
    }
}
