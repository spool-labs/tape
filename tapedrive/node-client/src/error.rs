//! Error types for node client operations.

use thiserror::Error;

/// Errors that can occur when communicating with storage nodes.
#[derive(Debug, Error)]
pub enum NodeError {
    /// HTTP request failed.
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),

    /// URL parsing failed.
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),

    /// Server returned an error status.
    #[error("server error: {status} - {message}")]
    ServerError {
        status: u16,
        message: String,
    },

    /// Slice not found on this node.
    #[error("slice not found")]
    NotFound,

    /// Node is not responsible for this spool.
    #[error("node not responsible for this spool")]
    NotResponsible,

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Connection timeout.
    #[error("connection timeout")]
    Timeout,

    /// TLS/certificate error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Invalid response from server.
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

impl NodeError {
    /// Create a server error from status code and message.
    pub fn server_error(status: u16, message: impl Into<String>) -> Self {
        Self::ServerError {
            status,
            message: message.into(),
        }
    }

    /// Check if error is retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            NodeError::Request(e) => e.is_timeout() || e.is_connect(),
            NodeError::Timeout => true,
            NodeError::ServerError { status, .. } => {
                // 5xx errors are potentially retryable
                *status >= 500 && *status < 600
            }
            _ => false,
        }
    }
}
