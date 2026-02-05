//! Blob decoding from network slices.
//!
//! This module provides `BlobDecoder` which reconstructs original blobs
//! from downloaded slices using erasure code decoding.

use tape_core::prelude::EncodingType;
use tape_slicer::{BasicSlicer, RotatedSlicer, Blob, Slice, SliceIndex, Slicer, SLICE_COUNT, DATA_SLICES};

use crate::error::DownloadError;

/// Decodes slices back into the original blob.
///
/// Supports multiple encoding types:
/// - `Basic`: Single RS pass, for testing/debugging only
/// - `Clay`: Clay erasure codes with rotation for fair load distribution (default)
///
/// Reconstructs the original data from any DATA_SLICES (or more) valid slices.
pub struct BlobDecoder {
    encoding_type: EncodingType,
    basic: Option<BasicSlicer>,
    clay: Option<RotatedSlicer>,
}

impl Default for BlobDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobDecoder {
    /// Create a new decoder with default encoding type (Clay).
    pub fn new() -> Self {
        Self::with_encoding(EncodingType::Clay)
    }

    /// Create a decoder with a specific encoding type.
    ///
    /// # Arguments
    /// * `encoding_type` - The encoding algorithm used for the slices
    pub fn with_encoding(encoding_type: EncodingType) -> Self {
        let mut decoder = Self {
            encoding_type,
            basic: None,
            clay: None,
        };

        match encoding_type {
            EncodingType::Basic => {
                decoder.basic = Some(BasicSlicer::default());
            }
            EncodingType::Clay | EncodingType::Unknown => {
                decoder.clay = Some(RotatedSlicer::default());
            }
        }

        decoder
    }

    /// Get the encoding type used by this decoder.
    pub fn encoding_type(&self) -> EncodingType {
        self.encoding_type
    }

    /// Internal decoding dispatch.
    fn decode_internal(&mut self, slice_array: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DownloadError> {
        match self.encoding_type {
            EncodingType::Basic => {
                self.basic.as_mut().unwrap()
                    .decode(slice_array)
                    .map_err(|e| DownloadError::Decoding(e.to_string()))
            }
            EncodingType::Clay | EncodingType::Unknown => {
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
    /// Requires at least DATA_SLICES valid slices for reconstruction.
    /// Extra slices beyond DATA_SLICES are used but not required.
    ///
    /// # Arguments
    /// * `slices` - Vector of (index, data) tuples from downloaded slices
    ///
    /// # Returns
    /// The reconstructed original blob data.
    ///
    /// # Errors
    /// - `InvalidSliceIndex` if any slice index >= SLICE_COUNT
    /// - `InsufficientSlices` if fewer than DATA_SLICES provided
    /// - `Decoding` if erasure code reconstruction fails
    pub fn decode(&mut self, slices: Vec<(u16, Vec<u8>)>) -> Result<Vec<u8>, DownloadError> {
        // Check we have enough slices
        if slices.len() < DATA_SLICES {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: DATA_SLICES,
            });
        }

        // Convert to the format expected by slicer
        let mut slice_array: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);

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
        // Check we have enough slices
        if slices.len() < DATA_SLICES {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: DATA_SLICES,
            });
        }

        // Convert to the format expected by slicer
        let mut slice_array: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);

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
    use tape_core::erasure::DATA_SLICES;

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

        // Keep only exactly DATA_SLICES (minimum required)
        let data_only: Vec<_> = slices.into_iter().take(DATA_SLICES).collect();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(data_only).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_with_missing_parity() {
        let original = vec![0xEF; 20_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only the first DATA_SLICES (all data, no parity)
        let data_only: Vec<_> = slices.into_iter().take(DATA_SLICES).collect();

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

        // Make sure we have enough
        assert!(scattered.len() >= DATA_SLICES);

        let mut decoder = test_decoder();
        let recovered = decoder.decode(scattered).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_not_enough_slices() {
        let original = vec![0x34; 10_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original).unwrap();

        // Only keep DATA_SLICES - 1 slices (not enough)
        let too_few: Vec<_> = slices.into_iter().take(DATA_SLICES - 1).collect();

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

        // Replace one slice's index with an invalid one (>= SLICE_COUNT)
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
