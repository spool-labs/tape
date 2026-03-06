//! Error types for SDK operations.

use tape_core::spooler::SpoolIndex;
use tape_core::types::StorageUnits;
use tape_peer::PeerError;
use thiserror::Error;

use crate::certification::CertificationError;
use crate::network::NetworkError;

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

    #[error("peer error: {0}")]
    Peer(#[from] PeerError),

    #[error("no nodes available")]
    NoNodesAvailable,

    #[error("semaphore error")]
    Semaphore,

    #[error("slice encoding failed: {0}")]
    Encoding(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("invalid slice count: expected {expected}, got {got}")]
    InvalidSliceCount { expected: usize, got: usize },
}

/// Errors that can occur during download.
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("insufficient slices: got {got}, need {need}")]
    InsufficientSlices { got: usize, need: usize },

    #[error("node error: {0}")]
    Node(String),

    #[error("no nodes available")]
    NoNodesAvailable,

    #[error("committee not found for epoch {0}")]
    CommitteeNotFound(u64),

    #[error("slice verification failed")]
    VerificationFailed,

    #[error("slice decoding failed: {0}")]
    Decoding(String),

    #[error("invalid slice index: {0}")]
    InvalidSliceIndex(SpoolIndex),
}

/// Errors from the high-level [`Tapedrive`](crate::Tapedrive) client.
#[derive(Debug, Error)]
pub enum TapedriveError {
    #[error("RPC error: {0}")]
    Rpc(#[from] rpc_client::RpcError),

    #[error("upload failed: {0}")]
    Upload(#[from] UploadError),

    #[error("download failed: {0}")]
    Download(#[from] ClientError),

    #[error("certification failed: {0}")]
    Certification(#[from] CertificationError),

    #[error("network error: {0}")]
    Network(#[from] NetworkError),

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("commitment mismatch")]
    CommitmentMismatch,

    #[error("not found")]
    NotFound,

    #[error("insufficient capacity: need {need}, available {available}")]
    InsufficientCapacity {
        need: StorageUnits,
        available: StorageUnits,
    },

    #[error("{0}")]
    InvalidArgument(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
