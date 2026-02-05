#![allow(clippy::len_without_is_empty)]

pub mod errors;
pub mod merkle_helpers;
pub mod reed_solomon;
pub mod clay;
pub mod slice_index;
pub mod striped;

pub use merkle_helpers::MERKLE_HEIGHT;
pub use tape_core::erasure::SPOOL_GROUP_SIZE;
pub use errors::{EncodeError, DecodeError};
pub use clay::ClayCoder;
pub use reed_solomon::ReedSolomonCoder;
pub use striped::{StripedCoder, SliceMetadata, MappingStrategy, ROTATION_STEP, STRIPE_SIZES, pick_stripe_size};

// Re-export encoding types from core for convenience
pub use tape_core::encoding::{EncodingProfile, EncodingType, ClayParams, RSParams};
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use reed_solomon::MAX_SLICE_BYTES;

/// Unified trait for erasure code encoding/decoding.
///
/// Implementations include:
/// - `ClayCoder`: Raw Clay MSR codes (k data, m parity)
/// - `ReedSolomonCoder`: Raw Reed-Solomon codes
/// - `StripedCoder<C>`: Wraps any Slicer with striping + metadata + optional rotation
pub trait Slicer {
    /// Data chunks (k) needed for reconstruction.
    fn k(&self) -> usize;

    /// Parity chunks (m).
    fn m(&self) -> usize;

    /// Total chunks (n = k + m).
    fn n(&self) -> usize {
        self.k() + self.m()
    }

    /// Encode data into n chunks.
    ///
    /// # Arguments
    /// * `data` - Raw bytes to encode
    ///
    /// # Returns
    /// Vector of n chunks (each chunk is a Vec<u8>).
    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError>;

    /// Decode from available chunks.
    ///
    /// # Arguments
    /// * `chunks` - Sparse array of (chunk_index, chunk_data) pairs.
    ///              Must have at least k chunks for reconstruction.
    ///
    /// # Returns
    /// Reconstructed original data.
    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError>;
}
