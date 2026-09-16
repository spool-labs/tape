//! A service that could not start

use std::io;
use std::net::SocketAddr;

use thiserror::Error;

use crate::api::error::ApiError;
use crate::chain::error::ChainError;
use crate::config::error::ConfigError;
use crate::records::error::RecordsError;
use crate::worker::error::WorkerError;

/// Startup failures; running components log their own errors
#[derive(Debug, Error)]
pub enum ServiceError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Records(#[from] RecordsError),

    #[error(transparent)]
    Chain(#[from] ChainError),

    #[error(transparent)]
    Worker(#[from] WorkerError),

    #[error(transparent)]
    Api(#[from] ApiError),

    #[error("binding {address}: {source}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },

    #[error("listening for shutdown signals: {0}")]
    Signal(#[source] io::Error),

    #[error("{name} task failed: {reason}")]
    Task { name: &'static str, reason: String },
}
