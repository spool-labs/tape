#![allow(clippy::len_without_is_empty)]

pub mod adaptive;
pub mod coder;
pub mod errors;
pub mod merkle_helpers;
pub mod metadata;
pub mod outer;
pub mod reed_solomon;
pub mod clay;
pub mod repair;
pub mod slice_index;
pub mod slicer;

pub use merkle_helpers::MERKLE_HEIGHT;
pub use errors::{EncodeError, DecodeError, RepairError};
pub use coder::ErasureCoder;
pub use clay::ClayCoder;
pub use reed_solomon::ReedSolomonCoder;
pub use metadata::SliceMetadata;
pub use slicer::{Slicer, MappingStrategy, ROTATION_STEP, shard_to_slice, slice_to_shard};
pub use adaptive::{STRIPE_SIZES, DEFAULT_STRIPE_SIZE, pick_stripe_size, num_stripes};
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use repair::{RepairPlan, StripeRepair, HelperPlan, extract_repair_data};
pub use reed_solomon::MAX_SLICE_BYTES;
pub use outer::{OuterCoder, DEFAULT_K_OUTER, MAX_CHUNK_BYTES, SNAPSHOT_K_OUTER};
