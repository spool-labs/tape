//! Slice metadata for encoding parameters.
//!
//! Each encoded slice has a metadata suffix containing the information
//! needed to decode the blob: original length, stripe size, and encoding profile.

use bytemuck::{Pod, Zeroable};
use tape_core::encoding::EncodingProfile;
use tape_core::types::ChunkNumber;

use crate::adaptive::STRIPE_SIZES;
use crate::errors::DecodeError;

/// Metadata suffix appended to each slice.
///
/// Contains information needed to decode the blob:
/// - `version`: Format version for future extensibility
/// - `blob_len`: Original unencoded blob size in bytes
/// - `stripe_size`: Stripe size used during encoding
/// - `profile`: Encoding profile (type + params, 16 bytes)
/// - `chunk_index`: Position-dependent salt ensuring identical data chunks
///   (e.g. trailing zero-padded outer RS chunks) produce distinct commitments.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SliceMetadata {
    /// Format version (currently 0).
    pub version: u64,
    /// Original blob length in bytes.
    pub blob_len: u64,
    /// Stripe size used for encoding (one of STRIPE_SIZES).
    pub stripe_size: u64,
    /// Encoding profile (type + params).
    pub profile: EncodingProfile,
    /// Chunk/group index, ensures unique commitments per position even when
    /// multiple outer-RS chunks contain identical data (e.g. zero padding).
    pub chunk_index: ChunkNumber,
}

impl SliceMetadata {
    /// Current metadata format version.
    pub const VERSION: u64 = 0;

    /// Size of serialized metadata in bytes.
    pub const SIZE: usize = core::mem::size_of::<Self>(); // 48 bytes

    /// Create metadata for encoding with default Clay profile.
    pub fn new(blob_len: usize, stripe_size: usize) -> Self {
        Self::with_profile(blob_len, stripe_size, EncodingProfile::clay_default())
    }

    /// Create metadata with a specific encoding profile.
    pub fn with_profile(blob_len: usize, stripe_size: usize, profile: EncodingProfile) -> Self {
        Self {
            version: Self::VERSION,
            blob_len: blob_len as u64,
            stripe_size: stripe_size as u64,
            profile,
            chunk_index: ChunkNumber(0),
        }
    }

    /// Serialize to bytes for appending to slice.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        bytemuck::bytes_of(self).try_into().unwrap()
    }

    /// Parse from slice suffix bytes.
    pub fn from_slice(slice_data: &[u8]) -> Result<Self, DecodeError> {
        if slice_data.len() < Self::SIZE {
            return Err(DecodeError::InvalidLayout);
        }
        let suffix = &slice_data[slice_data.len() - Self::SIZE..];

        // Copy to aligned buffer for safe Pod conversion
        let mut buf = [0u8; Self::SIZE];
        buf.copy_from_slice(suffix);
        let meta: Self = *bytemuck::from_bytes(&buf);

        if !STRIPE_SIZES.contains(&(meta.stripe_size as usize)) {
            return Err(DecodeError::InvalidLayout);
        }

        Ok(meta)
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

    /// Get the encoding profile.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
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
        assert_eq!(SliceMetadata::SIZE, 48);
    }

    #[test]
    fn test_roundtrip() {
        let mut meta = SliceMetadata::new(12345, STRIPE_SIZES[0]);
        meta.chunk_index = ChunkNumber(42);
        let bytes = meta.to_bytes();

        // Simulate slice with metadata suffix
        let mut slice = vec![0u8; 100];
        slice.extend_from_slice(&bytes);

        let parsed = SliceMetadata::from_slice(&slice).unwrap();
        assert_eq!(parsed.blob_len(), 12345);
        assert_eq!(parsed.stripe_size(), STRIPE_SIZES[0]);
        assert_eq!(parsed.version(), SliceMetadata::VERSION);
        assert_eq!(parsed.chunk_index(), ChunkNumber(42));
    }

    #[test]
    fn test_too_short() {
        let short = vec![0u8; SliceMetadata::SIZE - 1];
        assert!(SliceMetadata::from_slice(&short).is_err());
    }

    #[test]
    fn test_invalid_stripe() {
        let mut meta = SliceMetadata::new(1000, STRIPE_SIZES[0]);
        meta.stripe_size = 999; // invalid
        let bytes = meta.to_bytes();

        let result = SliceMetadata::from_slice(&bytes);
        assert!(result.is_err());
    }
}
