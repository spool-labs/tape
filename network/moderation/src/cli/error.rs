//! Commands that failed

use thiserror::Error;

use crate::config::error::ConfigError;
use crate::operator::error::OperatorError;
use crate::service::error::ServiceError;

/// A command that did not finish
#[derive(Debug, Error)]
pub enum CliError {
    #[error("pass --config <yaml>")]
    MissingConfig,

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Service(#[from] ServiceError),

    #[error(transparent)]
    Operator(#[from] OperatorError),
}
