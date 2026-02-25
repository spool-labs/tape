//! High-level SDK for tapedrive blob upload/download operations.
//!
//! This crate provides two main client types:
//! - [`TapeClient`]: High-level blob upload/download operations
//! - [`RpcClient`]: On-chain state queries (re-exported from `rpc-client`)
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
//! use tape_sdk::{TapeClient, RpcClient, RpcConfig};
//!
//! // Create tape client with storage node addresses
//! let client = TapeClient::new(vec![
//!     "node1.example.com:8080".to_string(),
//!     "node2.example.com:8080".to_string(),
//! ]);
//!
//! // Upload a blob
//! let data = vec![0u8; 1_000_000]; // 1 MB of data
//! let commitment = client.upload_blob("my-track-id", data).await?;
//!
//! // Download the blob (k=10 from on-chain track profile)
//! let recovered = client.download_blob("my-track-id", 10).await?;
//!
//! // Query on-chain state
//! let rpc = RpcClient::new(RpcConfig::default())?;
//! let tape = rpc.get_tape_by_number(TapeNumber(42)).await?;
//! ```

pub mod certification;
pub mod client;
pub mod communication;
pub mod decoder;
pub mod discovery;
pub mod downloader;
pub mod encoder;
pub mod error;
pub mod helpers;
pub mod routing;
pub mod tape_key;
pub mod tapedrive;
pub mod uploader;

// High-level SDK interface
pub use tape_key::TapeKey;
pub use tapedrive::Tapedrive;

// Primary client interface for blob operations
pub use client::{TapeClient, TapeClientBuilder};

// Encoder/Decoder for direct use
pub use decoder::BlobDecoder;
pub use encoder::{BlobEncoder, SliceMerkleProof};

// Lower-level upload/download
pub use downloader::ParallelDownloader;
pub use uploader::{DistributedUploader, SliceWithProof};

// Error types
pub use error::{ClientError, DownloadError, TapedriveError, UploadError};

// Certification (BLS signature collection for tracks)
pub use certification::{
    CertificationCollector, CertificationConfig, CertificationError, CollectedSignatures,
};

// Routing
pub use routing::{RoutingError, SliceRouter};

// Discovery (on-chain node address resolution)
pub use discovery::{
    discover_committee_addresses, discover_committee_addresses_required,
    discover_full, discover_full_required,
    DiscoveryError, DiscoveryResult, NetworkState,
};

// Helpers (keypair loading, hash parsing, committee operations)
pub use helpers::{
    HelperError,
    load_solana_keypair, load_bls_keypair, load_tls_keypair,
    parse_hash, parse_hex_bytes,
    find_member_index, get_node_assigned_spools,
    create_rpc_client, create_rpc_client_with_config,
};

// Re-export RPC client types for on-chain queries
pub use rpc_client::RpcClient;
pub use rpc_client::{Rpc, RpcConfig, RpcError, SolanaRpc};

// Re-export key constants from tape-core for convenience
pub use tape_core::erasure::{MAX_BLOB_SIZE, MAX_SLICE_SIZE, SPOOL_GROUP_SIZE, SPOOL_COUNT};

// Re-export merkle types from tape-slicer
pub use tape_slicer::{BlobMerkleRoot, MERKLE_HEIGHT};

// Re-export payload type from tape-node-api
pub use tape_node_api::SlicePayload;
