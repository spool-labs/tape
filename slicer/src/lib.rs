#![allow(clippy::len_without_is_empty)]

pub mod errors;
pub mod types;
pub mod api;
pub mod basic;
pub mod codec;
pub mod striped;
pub mod rotated;
pub mod merkle_helpers;
pub mod reed_solomon;
pub mod clay;
pub mod slice_index;

pub use merkle_helpers::MERKLE_HEIGHT;
pub use tape_core::erasure::SPOOL_GROUP_SIZE;
pub use errors::{EncodeError, DecodeError};
pub use types::{Slice, Blob};
pub use api::Slicer;
pub use basic::BasicSlicer;
pub use codec::{DEFAULT_STRIPE_SIZE, ROTATION_STEP, STRIPE_SIZES, pick_stripe_size, SliceMetadata, StripedCodec};
pub use clay::{ClayCoder, ClayEncodeError, ClayDecodeError};
pub use reed_solomon::{ReedSolomonCoder, ReedSolomonEncodeError, ReedSolomonDecodeError, RawSlices};

// Re-export encoding types from core for convenience
pub use tape_core::encoding::{EncodingProfile, EncodingType, ClayParams, RSParams};
pub use striped::StripedSlicer;
pub use rotated::RotatedSlicer;
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use reed_solomon::MAX_SLICE_BYTES;
