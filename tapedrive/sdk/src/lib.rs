//! High-level SDK for tapedrive blob upload/download operations.
//!
//! This crate provides `TapeClient` for uploading and downloading blobs
//! to/from the tapedrive network.
//!
//! # Overview
//!
//! The SDK integrates three main components:
//! - **Encoder/Decoder**: Reed-Solomon erasure coding via `tape-slicer`
//! - **Uploader/Downloader**: Parallel slice distribution to storage nodes
//! - **TapeClient**: High-level interface combining encoding and network operations
//!
//! # Example
//!
//! ```rust,ignore
//! use tape_sdk::TapeClient;
//!
//! // Create client with storage node addresses
//! let client = TapeClient::new(vec![
//!     "node1.example.com:8080".to_string(),
//!     "node2.example.com:8080".to_string(),
//! ]);
//!
//! // Upload a blob
//! let data = vec![0u8; 1_000_000]; // 1 MB of data
//! let commitment = client.upload_blob("my-track-id", data).await?;
//!
//! // Download the blob
//! let recovered = client.download_blob("my-track-id").await?;
//! ```

pub mod client;
pub mod communication;
pub mod decoder;
pub mod downloader;
pub mod encoder;
pub mod error;
pub mod uploader;

// Primary client interface
pub use client::TapeClient;

// Encoder/Decoder for direct use
pub use decoder::BlobDecoder;
pub use encoder::{BlobEncoder, SliceMerkleProof};

// Lower-level upload/download
pub use downloader::ParallelDownloader;
pub use uploader::DistributedUploader;

// Error types
pub use error::{ClientError, DownloadError, UploadError};

// Re-export key constants from tape-core for convenience
pub use tape_core::erasure::{DATA_SLICES, MAX_BLOB_SIZE, MAX_SLICE_SIZE, SLICE_COUNT};

// Re-export merkle types from tape-slicer
pub use tape_slicer::{BlobMerkleRoot, MERKLE_HEIGHT};
