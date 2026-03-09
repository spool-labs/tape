//! High-level SDK for tapedrive blob upload/download operations.

pub mod codec;
pub mod error;
pub mod keys;
pub mod staking;
pub mod tapedrive;
pub mod transfer;

pub use tape_protocol::peer::{PeerManager, PeerManagerError, PeerNode};
pub use keys::stake_key::StakeKey;
pub use keys::tape_key::TapeKey;
pub use tapedrive::Tapedrive;

pub use codec::decoder::BlobDecoder;
pub use codec::encoder::{BlobEncoder, SliceMerkleProof};

pub use transfer::downloader::ParallelDownloader;
pub use transfer::uploader::{DistributedUploader, SliceWithProof};

pub use error::{ClientError, DownloadError, TapedriveError, UploadError};

pub use transfer::certify::{
    CertificationCollector, CertificationConfig, CertificationError, CollectedSignatures,
};

pub use keys::helpers::{
    HelperError,
    load_solana_keypair, load_bls_keypair, load_tls_keypair,
    parse_hash, parse_hex_bytes,
    find_member_index, get_node_assigned_spools,
    create_rpc_client, create_rpc_client_with_config,
};

pub use rpc_client::RpcClient;
pub use rpc_client::{Rpc, RpcConfig, RpcError, SolanaRpc};
pub use peer_http::HttpApi;
pub use tape_core::erasure::{MAX_BLOB_SIZE, MAX_SLICE_SIZE, SPOOL_GROUP_SIZE, SPOOL_COUNT};
pub use tape_slicer::{BlobMerkleRoot, MERKLE_HEIGHT};
pub use tape_protocol::api::SlicePayload;
