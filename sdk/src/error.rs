//! Error types for SDK operations.

use std::time::Duration;

use tape_core::types::{SpoolIndex, StorageUnits};
use tape_protocol::ApiError;
use thiserror::Error;
use rpc::RpcError;

use crate::transfer::certify::CertificationError;
use peer_manager::PeerManagerError;

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

    #[error("insufficient slices: got {got}, need {need}")]
    InsufficientSlices { got: usize, need: usize },

    #[error("peer error: {0}")]
    Peer(#[from] ApiError),

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

    #[error("epoch changed during upload: {not_responsible} slices rejected")]
    EpochChanged { not_responsible: usize },
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
    #[error("a payer keypair is required for this operation")]
    MissingPayer,

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("upload failed: {0}")]
    Upload(#[from] UploadError),

    #[error("download failed: {0}")]
    Download(#[from] ClientError),

    #[error("certification failed: {0}")]
    Certification(#[from] CertificationError),

    #[error("network error: {0}")]
    Network(#[from] PeerManagerError),

    #[error("peer error: {0}")]
    Peer(ApiError),

    #[error("rate limited")]
    RateLimited { retry_after: Option<Duration> },

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

    #[error("stream error: {0}")]
    Stream(String),
}

/// Rate limiting gets its own variant so callers can back off without string matching.
impl From<ApiError> for TapedriveError {
    fn from(error: ApiError) -> Self {
        match error {
            ApiError::RateLimited { retry_after } => Self::RateLimited { retry_after },
            other => Self::Peer(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // rate limited api errors map to the dedicated variant
    #[test]
    fn rate_limited() {
        let error = TapedriveError::from(ApiError::RateLimited {
            retry_after: Some(Duration::from_secs(3)),
        });
        assert!(matches!(
            error,
            TapedriveError::RateLimited { retry_after: Some(retry) } if retry.as_secs() == 3
        ));

        let error = TapedriveError::from(ApiError::NotFound);
        assert!(matches!(error, TapedriveError::Peer(ApiError::NotFound)));
    }
}
