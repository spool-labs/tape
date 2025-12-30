#![allow(clippy::len_without_is_empty)]

pub mod consts;
pub mod errors;
pub mod types;
pub mod api;
pub mod basic;
pub mod striped;
pub mod merkle_helpers;
pub mod reed_solomon;
pub mod slice_index;

pub use consts::{MERKLE_HEIGHT, SLICE_COUNT, F, CODING_SLICES, DATA_SLICES};
pub use errors::{EncodeError, DecodeError};
pub use types::{Slice, Blob};
pub use api::Slicer;
pub use basic::BasicSlicer;
pub use striped::StripedSlicer;
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use reed_solomon::DEFAULT_MAX_SLICE_BYTES;
