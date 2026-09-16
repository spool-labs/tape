//! Operator commands that failed

use thiserror::Error;

use crate::chain::error::ChainError;
use crate::funder::error::FunderError;
use crate::records::error::RecordsError;

/// An operator command that could not finish
#[derive(Debug, Error)]
pub enum OperatorError {
    #[error(transparent)]
    Records(#[from] RecordsError),

    #[error(transparent)]
    Chain(#[from] ChainError),

    #[error(transparent)]
    Funder(#[from] FunderError),
}
