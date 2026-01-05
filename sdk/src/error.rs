//! Error types for SDK operations.

use tape_node_client::NodeError;
use thiserror::Error;

/// Errors that can occur during client operations.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("upload failed: {0}")]
    Upload(#[from] UploadError),

    #[error("download failed: {0}")]
    Download(#[from] DownloadError),

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("decoding error: {0}")]
    Decoding(String),

    #[error("commitment mismatch")]
    CommitmentMismatch,

    #[error("track not found")]
    TrackNotFound,

    #[error("committee not available")]
    CommitteeNotAvailable,

    #[error("RPC error: {0}")]
    Rpc(String),
}

/// Errors that can occur during upload.
#[derive(Debug, Error)]
pub enum UploadError {
    #[error("insufficient quorum: got {got}, need {need}")]
    InsufficientQuorum { got: usize, need: usize },

    #[error("node error: {0}")]
    Node(#[from] NodeError),

    #[error("no nodes available")]
    NoNodesAvailable,

    #[error("semaphore error")]
    Semaphore,

    #[error("slice encoding failed: {0}")]
    Encoding(String),
}

/// Errors that can occur during download.
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("insufficient slices: got {got}, need {need}")]
    InsufficientSlices { got: usize, need: usize },

    #[error("node error: {0}")]
    Node(#[from] NodeError),

    #[error("no nodes available")]
    NoNodesAvailable,

    #[error("committee not found for epoch {0}")]
    CommitteeNotFound(u64),

    #[error("slice verification failed")]
    VerificationFailed,

    #[error("slice decoding failed: {0}")]
    Decoding(String),

    #[error("invalid slice index: {0}")]
    InvalidSliceIndex(u16),
}
