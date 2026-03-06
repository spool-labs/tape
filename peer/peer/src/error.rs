//! Error types for peer operations.

use tape_core::types::NodeId;

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
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
