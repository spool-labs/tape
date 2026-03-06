//! High-level SDK for tapedrive blob upload/download operations.

pub mod certification;
pub mod decoder;
pub mod downloader;
pub mod encoder;
pub mod error;
pub mod helpers;
pub mod network;
pub mod routing;
pub mod stake_key;
pub mod staking;
pub mod tape_key;
pub mod tapedrive;
pub mod uploader;

// High-level SDK interface
pub use network::{Network, NetworkError};
pub use stake_key::StakeKey;
pub use tape_key::TapeKey;
pub use tapedrive::Tapedrive;

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

// Re-export peer client types
pub use peer_http::HttpPeerClient;

// Re-export key constants from tape-core for convenience
pub use tape_core::erasure::{MAX_BLOB_SIZE, MAX_SLICE_SIZE, SPOOL_GROUP_SIZE, SPOOL_COUNT};

// Re-export merkle types from tape-slicer
pub use tape_slicer::{BlobMerkleRoot, MERKLE_HEIGHT};

// Re-export payload type from tape-node-api
pub use tape_node_api::SlicePayload;
