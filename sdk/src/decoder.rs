//! Blob decoding from network slices.
//!
//! This module provides `BlobDecoder` which reconstructs original blobs
//! from downloaded slices using Reed-Solomon decoding.

use tape_slicer::{BasicSlicer, Blob, Slice, SliceIndex, Slicer, SLICE_COUNT, DATA_SLICES};

use crate::error::DownloadError;

/// Decodes slices back into the original blob.
///
/// Uses Reed-Solomon erasure coding to reconstruct the original data
/// from any DATA_SLICES (or more) valid slices.
pub struct BlobDecoder {
    slicer: BasicSlicer,
}

impl Default for BlobDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobDecoder {
    /// Create a new decoder with default max slice size (1 MiB).
    pub fn new() -> Self {
        Self {
            slicer: BasicSlicer::default(),
        }
    }

    /// Create a decoder with a custom max slice size.
    ///
    /// Use smaller values for testing to reduce memory usage.
    /// For production, use `new()` or `Default::default()`.
    pub fn with_max_slice_bytes(max_slice_bytes: usize) -> Self {
        Self {
            slicer: BasicSlicer::with_max_slice_bytes(max_slice_bytes),
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
    /// - `Decoding` if Reed-Solomon reconstruction fails
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

        let blob = self.slicer
            .decode(&slice_array)
            .map_err(|e| DownloadError::Decoding(e.to_string()))?;

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

        self.slicer
            .decode(&slice_array)
            .map_err(|e| DownloadError::Decoding(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::BlobEncoder;
    use tape_core::erasure::DATA_SLICES;

    /// Smaller max slice size for testing to reduce memory usage.
    /// 4 KiB allows encoding blobs up to ~2.7 MB (DATA_SLICES * 4 KiB).
    const TEST_MAX_SLICE_BYTES: usize = 1 << 12; // 4 KiB

    /// Create test encoder/decoder pair with reduced memory footprint.
    fn test_encoder() -> BlobEncoder {
        BlobEncoder::with_max_slice_bytes(TEST_MAX_SLICE_BYTES)
    }

    fn test_decoder() -> BlobDecoder {
        BlobDecoder::with_max_slice_bytes(TEST_MAX_SLICE_BYTES)
    }

    #[test]
    fn test_roundtrip() {
        let original = vec![0xAB; 50_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let mut decoder = test_decoder();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_decode_with_only_data_slices() {
        let original = vec![0xCD; 30_000];

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
        let original = vec![0xEF; 25_000];

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
        let original = vec![0x12; 20_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        // Take every other slice, but ensure we have enough
        let scattered: Vec<_> = slices
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0 || *i < DATA_SLICES * 2)
            .map(|(_, s)| s)
            .take(DATA_SLICES + 10)
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

        // Only keep 100 slices (not enough - need at least DATA_SLICES)
        let too_few: Vec<_> = slices.into_iter().take(100).collect();

        let mut decoder = test_decoder();
        let result = decoder.decode(too_few);

        assert!(matches!(
            result,
            Err(DownloadError::InsufficientSlices { got: 100, need: _ })
        ));
    }

    #[test]
    fn test_decode_invalid_slice_index() {
        // Create slices with enough count but one has an invalid index (>= SLICE_COUNT)
        // First encode a real blob to get valid slices
        let original = vec![0x99; 10_000];
        let mut encoder = test_encoder();
        let mut slices: Vec<_> = encoder.encode(original).unwrap();

        // Replace one slice's index with an invalid one (>= SLICE_COUNT = 1024)
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
}
