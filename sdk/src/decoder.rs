//! Blob decoding from network slices.
//!
//! This module provides `BlobDecoder` which reconstructs original blobs
//! from downloaded slices using erasure code decoding.

use tape_core::encoding::{EncodingProfile, EncodingType, RSParams};
use tape_slicer::{
    BasicSlicer, RotatedSlicer, Blob, Slice, SliceIndex, SliceMetadata, Slicer,
    SPOOL_GROUP_SIZE, DEFAULT_STRIPE_SIZE,
};

use crate::error::DownloadError;

/// Default k value (matches default RSParams and ClayParams).
const DEFAULT_K: usize = 10;

/// Decodes slices back into the original blob.
///
/// Supports multiple encoding types:
/// - `Basic`: Single RS pass, for testing/debugging only
/// - `Clay`: Clay erasure codes with rotation for fair load distribution (default)
///
/// Reconstructs the original data from any k (or more) valid slices,
/// where k is determined from the slice metadata profile.
pub struct BlobDecoder {
    profile: EncodingProfile,
    basic: Option<BasicSlicer>,
    clay: Option<RotatedSlicer>,
}

impl Default for BlobDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobDecoder {
    /// Create a new decoder with default encoding profile (Clay).
    pub fn new() -> Self {
        Self::with_profile(EncodingProfile::clay_default())
    }

    /// Create a decoder with a specific encoding profile.
    ///
    /// # Arguments
    /// * `profile` - The encoding profile (type + params)
    pub fn with_profile(profile: EncodingProfile) -> Self {
        let encoding_type = profile.encoding_type().unwrap_or(EncodingType::Unknown);

        let mut decoder = Self {
            profile,
            basic: None,
            clay: None,
        };

        match encoding_type {
            EncodingType::Basic => {
                decoder.basic = Some(BasicSlicer::default());
            }
            EncodingType::Clay | EncodingType::Unknown => {
                decoder.clay = Some(RotatedSlicer::with_profile(DEFAULT_STRIPE_SIZE, profile));
            }
        }

        decoder
    }

    /// Create a decoder with a specific encoding type (uses default params for that type).
    ///
    /// # Arguments
    /// * `encoding_type` - The encoding algorithm used for the slices
    pub fn with_encoding(encoding_type: EncodingType) -> Self {
        let profile = match encoding_type {
            EncodingType::Basic => EncodingProfile::basic_default(),
            EncodingType::Clay | EncodingType::Unknown => EncodingProfile::clay_default(),
        };
        Self::with_profile(profile)
    }

    /// Get the encoding type used by this decoder.
    pub fn encoding_type(&self) -> EncodingType {
        self.profile.encoding_type().unwrap_or(EncodingType::Unknown)
    }

    /// Get the encoding profile used by this decoder.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }

    /// Get minimum slices needed for decoding from slice metadata.
    ///
    /// For Clay encoding, peeks at the first available slice to read its profile
    /// and determine k. For Basic encoding, uses profile.k() or default.
    fn min_slices_from_metadata(&self, slices: &[(u16, Vec<u8>)]) -> usize {
        match self.encoding_type() {
            EncodingType::Clay => {
                slices.first()
                    .and_then(|(_, data)| SliceMetadata::from_slice(data).ok())
                    .map(|meta| meta.clay_params().k() as usize)
                    .unwrap_or(DEFAULT_K)
            }
            // Basic encoding doesn't embed metadata, use profile k or default
            EncodingType::Basic => self.profile.rs_params().k() as usize,
            EncodingType::Unknown => DEFAULT_K,
        }
    }

    /// Internal decoding dispatch.
    fn decode_internal(&mut self, slice_array: &[Option<Slice>; SPOOL_GROUP_SIZE]) -> Result<Blob, DownloadError> {
        match self.encoding_type() {
            EncodingType::Basic => {
                self.basic.as_mut().unwrap()
                    .decode(slice_array)
                    .map_err(|e| DownloadError::Decoding(e.to_string()))
            }
            EncodingType::Clay | EncodingType::Unknown => {
                // StripedCodec::decode auto-reconfigures based on slice metadata
                self.clay.as_mut().unwrap()
                    .decode(slice_array)
                    .map_err(|e| DownloadError::Decoding(e.to_string()))
            }
        }
    }

    /// Decode slices back into the original blob.
    ///
    /// Takes a vector of (slice_index, slice_data) tuples as returned
    /// by `ParallelDownloader::download_enough_slices()`.
    ///
    /// Requires at least k valid slices for reconstruction (k from profile).
    /// Extra slices beyond k are used but not required.
    ///
    /// # Arguments
    /// * `slices` - Vector of (index, data) tuples from downloaded slices
    ///
    /// # Returns
    /// The reconstructed original blob data.
    ///
    /// # Errors
    /// - `InvalidSliceIndex` if any slice index >= SPOOL_GROUP_SIZE
    /// - `InsufficientSlices` if fewer than k slices provided (k from metadata)
    /// - `Decoding` if erasure code reconstruction fails
    pub fn decode(&mut self, slices: Vec<(u16, Vec<u8>)>) -> Result<Vec<u8>, DownloadError> {
        // Peek at metadata to get k (minimum slices needed)
        let min_slices = self.min_slices_from_metadata(&slices);

        // Check we have enough slices
        if slices.len() < min_slices {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: min_slices,
            });
        }

        // Convert to the format expected by slicer
        let mut slice_array: [Option<Slice>; SPOOL_GROUP_SIZE] = std::array::from_fn(|_| None);

        for (idx, data) in slices {
            let slice_idx = SliceIndex::new(idx as usize)
                .ok_or(DownloadError::InvalidSliceIndex(idx))?;
            slice_array[idx as usize] = Some(Slice::new(slice_idx, data));
        }

        let blob = self.decode_internal(&slice_array)?;

        Ok(blob.data)
    }

    /// Decode slices, returning the Blob wrapper type.
    ///
    /// Same as `decode()` but returns the slicer's `Blob` type instead
    /// of raw bytes. Useful when you need access to Blob methods.
    pub fn decode_to_blob(&mut self, slices: Vec<(u16, Vec<u8>)>) -> Result<Blob, DownloadError> {
        // Peek at metadata to get k (minimum slices needed)
        let min_slices = self.min_slices_from_metadata(&slices);

        // Check we have enough slices
        if slices.len() < min_slices {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: min_slices,
            });
        }

        // Convert to the format expected by slicer
        let mut slice_array: [Option<Slice>; SPOOL_GROUP_SIZE] = std::array::from_fn(|_| None);

        for (idx, data) in slices {
            let slice_idx = SliceIndex::new(idx as usize)
                .ok_or(DownloadError::InvalidSliceIndex(idx))?;
            slice_array[idx as usize] = Some(Slice::new(slice_idx, data));
        }

        self.decode_internal(&slice_array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::BlobEncoder;
    use tape_slicer::Slicer;

    // Default k=10 for BasicSlicer
    const K: usize = 10;

    /// Create test encoder using BasicSlicer (supports blobs up to ~40 KB).
    fn test_encoder() -> BlobEncoder {
        BlobEncoder::with_encoding(EncodingType::Basic)
    }

    /// Create test decoder using BasicSlicer.
    fn test_decoder() -> BlobDecoder {
        BlobDecoder::with_encoding(EncodingType::Basic)
    }

    #[test]
    fn test_roundtrip() {
        let original = vec![0xAB; 20_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_with_only_data_slices() {
        let original = vec![0xCD; 15_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only exactly k (minimum required)
        let data_only: Vec<_> = slices.into_iter().take(K).collect();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(data_only).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_with_missing_parity() {
        let original = vec![0xEF; 20_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only the first k (all data, no parity)
        let data_only: Vec<_> = slices.into_iter().take(K).collect();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(data_only).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_with_scattered_slices() {
        let original = vec![0x12; 10_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        // Take every other slice, but ensure we have enough
        let scattered: Vec<_> = slices
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, s)| s)
            .collect();

        // Make sure we have enough (k=10, so every other of 20 = 10)
        assert!(scattered.len() >= K);

        let mut decoder = test_decoder();
        let recovered = decoder.decode(scattered).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_not_enough_slices() {
        let original = vec![0x34; 10_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original).unwrap();

        // Only keep k - 1 slices (not enough)
        let too_few: Vec<_> = slices.into_iter().take(K - 1).collect();

        let mut decoder = test_decoder();
        let result = decoder.decode(too_few);

        assert!(matches!(
            result,
            Err(DownloadError::InsufficientSlices { .. })
        ));
    }

    #[test]
    fn test_decode_invalid_slice_index() {
        let original = vec![0x99; 10_000];
        let mut encoder = test_encoder();
        let mut slices: Vec<_> = encoder.encode(original).unwrap();

        // Replace one slice's index with an invalid one (>= SPOOL_GROUP_SIZE)
        slices[0].0 = 9999;

        let mut decoder = test_decoder();
        let result = decoder.decode(slices);

        assert!(matches!(result, Err(DownloadError::InvalidSliceIndex(9999))));
    }

    #[test]
    fn test_decode_empty_blob() {
        let original = vec![];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_to_blob() {
        let original = vec![0x56; 15_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let mut decoder = test_decoder();
        let blob = decoder.decode_to_blob(slices).unwrap();

        assert_eq!(original.len(), blob.len());
        assert_eq!(original.as_slice(), blob.as_slice());
    }

    #[test]
    fn test_encoding_type_default() {
        let decoder = BlobDecoder::new();
        assert_eq!(decoder.encoding_type(), EncodingType::Clay);
    }

    #[test]
    fn test_encoding_type_basic() {
        let decoder = BlobDecoder::with_encoding(EncodingType::Basic);
        assert_eq!(decoder.encoding_type(), EncodingType::Basic);
    }

    #[test]
    fn test_encoding_type_clay() {
        let decoder = BlobDecoder::with_encoding(EncodingType::Clay);
        assert_eq!(decoder.encoding_type(), EncodingType::Clay);
    }
}
