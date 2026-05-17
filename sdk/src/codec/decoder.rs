//! Blob decoding from network slices.
//!
//! This module provides `BlobDecoder` which reconstructs original blobs
//! from downloaded slices using erasure code decoding.

use tape_core::encoding::{EncodingProfile, EncodingType};
use tape_core::spooler::SpoolIndex;
use tape_core::erasure::GROUP_SIZE;
use tape_slicer::{
    ClayCoder, DEFAULT_STRIPE_SIZE, ErasureCoder, ReedSolomonCoder, Slicer, SliceMetadata,
};

use crate::error::DownloadError;

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
    basic: Option<ReedSolomonCoder>,
    clay: Option<Slicer<ClayCoder>>,
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
                let params = profile.rs_params();
                decoder.basic = Some(ReedSolomonCoder::new(params.k() as usize, params.m() as usize));
            }
            EncodingType::Clay | EncodingType::Unknown => {
                decoder.clay = Some(Slicer::with_profile(
                    ClayCoder::from_params(profile.clay_params()),
                    DEFAULT_STRIPE_SIZE,
                    true, // rotated
                    profile,
                ));
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
    /// For Clay encoding, peeks at the first available slice to read its profile.
    /// For Basic encoding, uses profile.k().
    ///
    /// # Errors
    /// Returns error if k cannot be determined (Unknown encoding or missing metadata).
    fn min_slices_from_metadata(&self, slices: &[(SpoolIndex, Vec<u8>)]) -> Result<usize, DownloadError> {
        match self.encoding_type() {
            EncodingType::Clay => {
                slices.first()
                    .and_then(|(_, data)| SliceMetadata::from_slice(data).ok())
                    .map(|meta| meta.profile().clay_params().k() as usize)
                    .ok_or_else(|| DownloadError::Decoding(
                        "Cannot determine k: no valid slice metadata".to_string()
                    ))
            }
            EncodingType::Basic => Ok(self.profile.rs_params().k() as usize),
            EncodingType::Unknown => Err(DownloadError::Decoding(
                "Cannot decode with Unknown encoding type".to_string()
            )),
        }
    }

    /// Internal decoding dispatch.
    fn decode_internal(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DownloadError> {
        match self.encoding_type() {
            EncodingType::Basic => {
                let result = self.basic.as_mut().unwrap()
                    .decode(chunks)
                    .map_err(|e| DownloadError::Decoding(e.to_string()))?;
                // RS decode may have padding, but for Basic encoding we just return it
                // The caller should know the original length
                Ok(result)
            }
            EncodingType::Clay | EncodingType::Unknown => {
                // Slicer::decode auto-reconfigures based on slice metadata
                self.clay.as_mut().unwrap()
                    .decode(chunks)
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
    /// - `InvalidSliceIndex` if any slice index >= GROUP_SIZE
    /// - `InsufficientSlices` if fewer than k slices provided (k from metadata)
    /// - `Decoding` if erasure code reconstruction fails or encoding type is Unknown
    pub fn decode(&mut self, slices: Vec<(SpoolIndex, Vec<u8>)>) -> Result<Vec<u8>, DownloadError> {
        // Peek at metadata to get k (minimum slices needed)
        let min_slices = self.min_slices_from_metadata(&slices)?;

        // Check we have enough slices
        if slices.len() < min_slices {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: min_slices,
            });
        }

        // Validate indices and build refs
        for &(idx, _) in &slices {
            if idx as usize >= GROUP_SIZE {
                return Err(DownloadError::InvalidSliceIndex(idx));
            }
        }

        // Convert to (usize, &[u8]) format expected by Slicer trait
        let chunks: Vec<(usize, &[u8])> = slices
            .iter()
            .map(|(idx, data)| (*idx as usize, data.as_slice()))
            .collect();

        self.decode_internal(&chunks)
    }

    /// Decode slices, returning the data as a Vec<u8>.
    ///
    /// Same as `decode()` - kept for API compatibility.
    pub fn decode_to_blob(&mut self, slices: Vec<(SpoolIndex, Vec<u8>)>) -> Result<Vec<u8>, DownloadError> {
        self.decode(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encoder::BlobEncoder;

    /// Create test encoder using ReedSolomonCoder (supports blobs up to ~40 KB).
    fn test_encoder() -> BlobEncoder {
        BlobEncoder::with_encoding(EncodingType::Basic)
    }

    /// Create test decoder using ReedSolomonCoder.
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

        // For Basic encoding, we need to trim padding
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_only_data_slices() {
        let original = vec![0xCD; 15_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only exactly k (minimum required)
        let data_only: Vec<_> = slices.into_iter().take(k).collect();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(data_only).unwrap();

        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_missing_parity() {
        let original = vec![0xEF; 20_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only the first k (all data, no parity)
        let data_only: Vec<_> = slices.into_iter().take(k).collect();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(data_only).unwrap();

        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_scattered_slices() {
        let original = vec![0x12; 10_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original.clone()).unwrap();

        // Take every other slice, but ensure we have enough
        let scattered: Vec<_> = slices
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, s)| s)
            .collect();

        assert!(scattered.len() >= k);

        let mut decoder = test_decoder();
        let recovered = decoder.decode(scattered).unwrap();

        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_not_enough_slices() {
        let original = vec![0x34; 10_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original).unwrap();

        // Only keep k - 1 slices (not enough)
        let too_few: Vec<_> = slices.into_iter().take(k - 1).collect();

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

        // Replace one slice's index with an invalid one (>= GROUP_SIZE)
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

        // Empty encodes to k minimal slices, decodes to zeros
        assert!(recovered.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_decode_to_blob() {
        let original = vec![0x56; 15_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let mut decoder = test_decoder();
        let blob = decoder.decode_to_blob(slices).unwrap();

        assert_eq!(&blob[..original.len()], &original);
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

    #[test]
    fn test_clay_roundtrip() {
        let original = vec![0xAB; 10_000];

        let mut encoder = BlobEncoder::with_encoding(EncodingType::Clay);
        let mut decoder = BlobDecoder::with_encoding(EncodingType::Clay);

        let slices = encoder.encode(original.clone()).unwrap();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_clay_decode_with_missing_slices() {
        let original = vec![0xCD; 50_000];

        let mut encoder = BlobEncoder::with_encoding(EncodingType::Clay);
        let mut decoder = BlobDecoder::with_encoding(EncodingType::Clay);

        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only first k slices (k=7 for default Clay)
        let k = encoder.profile().k() as usize;
        let partial: Vec<_> = slices.into_iter().take(k).collect();

        let recovered = decoder.decode(partial).unwrap();
        assert_eq!(original, recovered);
    }
}
