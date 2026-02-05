#![allow(clippy::len_without_is_empty)]

pub mod adaptive;
pub mod coder;
pub mod errors;
pub mod merkle_helpers;
pub mod metadata;
pub mod reed_solomon;
pub mod clay;
pub mod slice_index;
pub mod slicer;

pub use merkle_helpers::MERKLE_HEIGHT;
pub use tape_core::erasure::SPOOL_GROUP_SIZE;
pub use errors::{EncodeError, DecodeError};
pub use coder::ErasureCoder;
pub use clay::ClayCoder;
pub use reed_solomon::ReedSolomonCoder;
pub use metadata::SliceMetadata;
pub use slicer::{Slicer, MappingStrategy, ROTATION_STEP};
pub use adaptive::{STRIPE_SIZES, DEFAULT_STRIPE_SIZE, pick_stripe_size, num_stripes};

// Re-export encoding types from core for convenience
pub use tape_core::encoding::{EncodingProfile, EncodingType, ClayParams, RSParams};
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use reed_solomon::MAX_SLICE_BYTES;
