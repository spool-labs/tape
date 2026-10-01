//! Slice metadata for encoding parameters.
//!
//! Each encoded slice has a metadata suffix with what a decoder needs beyond
//! the network's profile: the original length and the stripe size.

use bytemuck::{Pod, Zeroable};
use tape_core::types::ChunkNumber;

use crate::errors::DecodeError;

/// Metadata suffix appended to each slice.
///
/// Contains information needed to decode the blob:
/// - `version`: Format version for future extensibility
/// - `blob_len`: Original unencoded blob size in bytes
/// - `stripe_size`: Stripe size used during encoding
/// - `chunk_index`: Position-dependent salt ensuring identical data chunks
///   (e.g. trailing zero-padded outer RS chunks) produce distinct commitments.
///
/// The code and its parameters aren't here. They are the network's profile,
/// fixed at genesis, and the slicer that reads a slice already holds them.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SliceMetadata {
    /// Format version (currently 0).
    pub version: u64,
    /// Original blob length in bytes.
    pub blob_len: u64,
    /// Stripe size used for encoding, derived from the blob length.
    pub stripe_size: u64,
    /// Chunk/group index, ensures unique commitments per position even when
    /// multiple outer-RS chunks contain identical data (e.g. zero padding).
    pub chunk_index: ChunkNumber,
}

impl SliceMetadata {
    /// Current metadata format version.
    pub const VERSION: u64 = 0;

    /// Size of serialized metadata in bytes.
    pub const SIZE: usize = core::mem::size_of::<Self>(); // 32 bytes

    /// Create metadata for a blob and the stripe size it encodes at.
    pub fn new(blob_len: usize, stripe_size: usize) -> Self {
        Self {
            version: Self::VERSION,
            blob_len: blob_len as u64,
            stripe_size: stripe_size as u64,
            chunk_index: ChunkNumber(0),
        }
    }

    /// Serialize to bytes for appending to slice.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        bytemuck::bytes_of(self).try_into().unwrap()
    }

    /// Parse from slice suffix bytes without checking the stripe size. A
    /// slicer checks it against its own coder and cap in `read_metadata`.
    pub fn parse(slice_data: &[u8]) -> Result<Self, DecodeError> {
        if slice_data.len() < Self::SIZE {
            return Err(DecodeError::InvalidLayout);
        }
        let suffix = &slice_data[slice_data.len() - Self::SIZE..];

        // Copy to aligned buffer for safe Pod conversion
        let mut buf = [0u8; Self::SIZE];
        buf.copy_from_slice(suffix);
        Ok(*bytemuck::from_bytes(&buf))
    }

    /// Get the format version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Get the original blob length in bytes.
    pub fn blob_len(&self) -> usize {
        self.blob_len as usize
    }

    /// Get the stripe size used for encoding.
    pub fn stripe_size(&self) -> usize {
        self.stripe_size as usize
    }

    /// Get the chunk index.
    pub fn chunk_index(&self) -> ChunkNumber {
        self.chunk_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size() {
        assert_eq!(SliceMetadata::SIZE, 32);
    }

    #[test]
    fn test_roundtrip() {
        let mut meta = SliceMetadata::new(12345, 12_600);
        meta.chunk_index = ChunkNumber(42);
        let bytes = meta.to_bytes();

        // Simulate slice with metadata suffix
        let mut slice = vec![0u8; 100];
        slice.extend_from_slice(&bytes);

        let parsed = SliceMetadata::parse(&slice).unwrap();
        assert_eq!(parsed.blob_len(), 12345);
        assert_eq!(parsed.stripe_size(), 12_600);
        assert_eq!(parsed.version(), SliceMetadata::VERSION);
        assert_eq!(parsed.chunk_index(), ChunkNumber(42));
    }

    #[test]
    fn test_too_short() {
        let short = vec![0u8; SliceMetadata::SIZE - 1];
        assert!(SliceMetadata::parse(&short).is_err());
    }
}
