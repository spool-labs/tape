//! High-level SDK for tapedrive blob upload/download operations.

pub mod codec;
pub mod error;
pub mod file;
pub mod keys;
pub mod staking;
pub mod tapedrive;
pub mod transfer;
mod tape;
mod track;

#[cfg(test)]
mod tests;

pub use keys::stake_key::StakeKey;
pub use keys::tape_key::TapeKey;
pub use tapedrive::Tapedrive;

pub use codec::decoder::BlobDecoder;
pub use codec::encoder::{BlobEncoder, SliceMerkleProof};

pub use transfer::downloader::ParallelDownloader;
pub use transfer::uploader::{DistributedUploader, SliceWithProof};
pub use track::write::{SDK_INLINE_RAW_MAX_BYTES, UploadPlan, WrittenTrack};

pub use error::{ClientError, DownloadError, TapedriveError, UploadError};
pub use file::error::FileError;
pub use file::receipt::FileReceipt;
pub use tape_protocol::api::FindTrackVersion;

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
