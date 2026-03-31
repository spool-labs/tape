//! Error types for API operations.

use tape_core::types::NodeId;
use tape_retry::Retryable;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("node not found in directory: {0:?}")]
    NodeUnresolved(NodeId),

    #[error("not found")]
    NotFound,

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("request timed out")]
    Timeout,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("server error {status}: {message}")]
    ServerError { status: u16, message: String },

    #[error("not responsible for this spool")]
    NotResponsible,

    #[error("not in committee")]
    NotInCommittee,

    #[error("peer error: {0}")]
    Other(String),
}

impl Retryable for ApiError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::ConnectionFailed(_) | Self::Timeout => true,
            Self::ServerError { status, .. } => matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            Self::NotFound
            | Self::NotResponsible
            | Self::NotInCommittee
            | Self::NodeUnresolved(_)
            | Self::Serialization(_)
            | Self::Other(_) => false,
        }
    }
}
