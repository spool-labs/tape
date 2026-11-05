//! Errors of the spool feature

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpoolError {
    #[error("store error: {0}")]
    Store(String),
    #[error("slicer error: {0}")]
    Slicer(String),
}
