//! High-level SDK for tapedrive blob upload/download operations.
//!
//! This crate provides two main client types:
//! - [`BlobClient`]: High-level blob upload/download operations
//! - [`RpcClient`]: On-chain state queries (re-exported from `tape-client`)
//!
//! # Overview
//!
//! The SDK integrates three main components:
//! - **Encoder/Decoder**: Reed-Solomon erasure coding via `tape-slicer`
//! - **Uploader/Downloader**: Parallel slice distribution to storage nodes
//! - **BlobClient**: High-level interface combining encoding and network operations
//!
//! # Example
//!
//! ```rust,ignore
//! use tape_sdk::{BlobClient, RpcClient, RpcConfig};
//!
//! // Create blob client with storage node addresses
//! let blob_client = BlobClient::new(vec![
//!     "node1.example.com:8080".to_string(),
//!     "node2.example.com:8080".to_string(),
//! ]);
//!
//! // Upload a blob
//! let data = vec![0u8; 1_000_000]; // 1 MB of data
//! let commitment = blob_client.upload_blob("my-track-id", data).await?;
//!
//! // Download the blob
//! let recovered = blob_client.download_blob("my-track-id").await?;
//!
//! // Query on-chain state
//! let rpc_client = RpcClient::new(RpcConfig::default())?;
//! let tape = rpc_client.get_tape_by_number(TapeNumber(42)).await?;
//! ```

pub mod client;
pub mod communication;
pub mod decoder;
pub mod discovery;
pub mod downloader;
pub mod encoder;
pub mod error;
pub mod helpers;
pub mod routing;
pub mod uploader;

// Primary client interface for blob operations
pub use client::{BlobClient, BlobClientBuilder, DEFAULT_MAX_SLICE_BYTES};

// Encoder/Decoder for direct use
pub use decoder::BlobDecoder;
pub use encoder::{BlobEncoder, SliceMerkleProof};

// Lower-level upload/download
pub use downloader::ParallelDownloader;
pub use uploader::{DistributedUploader, SliceWithProof};

// Error types
pub use error::{ClientError, DownloadError, UploadError};

// Routing
pub use routing::{RoutingError, SliceRouter};

// Discovery (on-chain node address resolution)
pub use discovery::{discover_committee_addresses, discover_committee_addresses_required, DiscoveryError, DiscoveryResult};

// Helpers (keypair loading, hash parsing, committee operations)
pub use helpers::{
    HelperError,
    load_solana_keypair, load_bls_keypair, load_tls_pubkey,
    parse_hash, parse_hex_bytes,
    find_member_index, get_node_assigned_spools,
    create_rpc_client, create_rpc_client_with_config,
};

// Re-export RPC client types for on-chain queries
// Note: Renamed to RpcClient to avoid confusion with TapeClient (blob operations)
pub use tape_client::TapeClient as RpcClient;
pub use tape_client::{RpcConfig, SolanaRpc};

// Re-export key constants from tape-core for convenience
pub use tape_core::erasure::{DATA_SLICES, MAX_BLOB_SIZE, MAX_SLICE_SIZE, SLICE_COUNT};

// Re-export merkle types from tape-slicer
pub use tape_slicer::{BlobMerkleRoot, MERKLE_HEIGHT};

// Re-export payload type from tape-node-api
pub use tape_node_api::SlicePayload;
