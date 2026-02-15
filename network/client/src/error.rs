//! Error types for node client operations.

/// Errors that can occur when communicating with storage nodes.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),

    #[error("server error {status}: {message}")]
    ServerError { status: u16, message: String },

    #[error("not found")]
    NotFound,

    #[error("node is not responsible for this spool")]
    NotResponsible,

    #[error("node is not in current committee")]
    NotInCommittee,

    #[error("missing slices: have {have}, need {need}")]
    MissingSlices { have: u16, need: u16 },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("connection error: {0}")]
    Connection(String),

    #[error("request timed out")]
    Timeout,

    #[error("TLS error: {0}")]
    Tls(String),

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

    /// Whether this error is transient and the operation should be retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Request(e) => e.is_timeout() || e.is_connect(),
            Self::ServerError { status, .. } => matches!(status, 429 | 502 | 503 | 504),
            Self::Connection(_) | Self::Timeout => true,
            Self::NotFound
            | Self::NotResponsible
            | Self::NotInCommittee
            | Self::MissingSlices { .. }
            | Self::Url(_)
            | Self::Serialization(_)
            | Self::Tls(_)
            | Self::InvalidResponse(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_retryable() {
        assert!(NodeError::server_error(503, "unavailable").is_retryable());
        assert!(NodeError::server_error(429, "rate limited").is_retryable());
        assert!(NodeError::Timeout.is_retryable());
        assert!(NodeError::Connection("reset".into()).is_retryable());

        assert!(!NodeError::NotFound.is_retryable());
        assert!(!NodeError::NotResponsible.is_retryable());
        assert!(!NodeError::server_error(400, "bad request").is_retryable());
        assert!(!NodeError::server_error(404, "not found").is_retryable());
    }
}
