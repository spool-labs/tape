//! Check and listing failures, which the worker retries

use thiserror::Error;

use crate::records::error::RecordsError;

/// A job attempt that did not finish
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("building http client: {0}")]
    Client(String),

    #[error("gateway request {path}: {source}")]
    GatewayRequest {
        path: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("gateway answered {status} for {path}")]
    GatewayStatus { path: String, status: u16 },

    #[error("gateway response for {path}: {reason}")]
    GatewayDecode { path: String, reason: String },

    #[error("moderation request: {0}")]
    ModerationRequest(#[source] reqwest::Error),

    #[error("moderation api answered {0}")]
    ModerationStatus(u16),

    #[error("moderation response: {0}")]
    ModerationDecode(String),

    #[error("moderation response is larger than {0} bytes")]
    ModerationTooLarge(usize),

    #[error(transparent)]
    Records(#[from] RecordsError),
}
