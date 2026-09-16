//! Metrics that could not be registered

use thiserror::Error;

/// A metric the registry refused
#[derive(Debug, Error)]
pub enum ObserveError {
    #[error("registering metrics: {0}")]
    Register(#[from] prometheus::Error),
}
