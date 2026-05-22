//! Error types for API operations.

use tape_crypto::Address;
use tape_retry::Retryable;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("node not found in directory: {0:?}")]
    NodeUnresolved(Address),

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

    #[error("object blacklisted by peer")]
    BlacklistedObject,

    #[error("not in committee")]
    NotInCommittee,

    #[error("stale track proof")]
    StaleTrackProof,

    #[error("peer error: {0}")]
    Other(String),
}

impl Retryable for ApiError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::ConnectionFailed(_) | Self::Timeout | Self::StaleTrackProof => true,
            Self::ServerError { status, .. } => matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            Self::NotFound
            | Self::NotResponsible
            | Self::BlacklistedObject
            | Self::NotInCommittee
            | Self::NodeUnresolved(_)
            | Self::Serialization(_)
            | Self::Other(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use tape_retry::Retryable;

    use super::ApiError;

    // A stale proof is a transient view mismatch and should be retried.
    #[test]
    fn stale_track_proof_is_retryable() {
        assert!(ApiError::StaleTrackProof.is_retryable());
    }

    #[test]
    fn blacklisted_object_is_not_retryable() {
        assert!(!ApiError::BlacklistedObject.is_retryable());
    }
}
