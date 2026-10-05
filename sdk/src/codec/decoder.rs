//! Blob decoding from network slices.
//!
//! This module provides `BlobDecoder` which reconstructs original blobs
//! from downloaded slices using erasure code decoding.

use tape_core::encoding::ErasureCode;
use tape_core::types::SpoolIndex;
use tape_slicer::{ErasureCoder, Slicer, StripedCoder};

use crate::error::DownloadError;

/// Decodes slices back into the original blob.
///
/// Reconstructs the original data from any k (or more) valid slices, with
/// the code and k taken from the network's profile.
pub struct BlobDecoder {
    profile: ErasureCode,
    slicer: Slicer<StripedCoder>,
}

impl BlobDecoder {
    /// Create the decoder for a network's profile.
    pub fn try_new(profile: ErasureCode) -> Result<Self, DownloadError> {
        let slicer = Slicer::try_from_profile(profile)
            .map_err(|e| DownloadError::Decoding(e.to_string()))?;

        Ok(Self { profile, slicer })
    }

    /// Get the encoding profile used by this decoder.
    pub fn profile(&self) -> ErasureCode {
        self.profile
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
    /// - `InvalidSliceIndex` if any slice index >= n
    /// - `InsufficientSlices` if fewer than k slices provided
    /// - `Decoding` if erasure code reconstruction fails
    pub fn decode(&self, slices: &[(SpoolIndex, &[u8])]) -> Result<Vec<u8>, DownloadError> {
        let min_slices = usize::from(self.profile.k());

        // Check we have enough slices
        if slices.len() < min_slices {
            return Err(DownloadError::InsufficientSlices {
                got: slices.len(),
                need: min_slices,
            });
        }

        // Validate indices and build refs
        let width = usize::from(self.profile.n());
        for &(idx, _) in slices {
            if idx.as_usize() >= width {
                return Err(DownloadError::InvalidSliceIndex(idx));
            }
        }

        let chunks: Vec<(usize, &[u8])> = slices
            .iter()
            .map(|&(idx, data)| (idx.as_usize(), data))
            .collect();

        self.slicer
            .decode(&chunks)
            .map_err(|e| DownloadError::Decoding(e.to_string()))
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{RSParams, RepParams, TAPENET_PROFILE};

    use crate::codec::encoder::BlobEncoder;

    /// A small Reed-Solomon profile: any 3 of 6 slices recover.
    const SMALL: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 3));

    fn refs(slices: &[(SpoolIndex, Vec<u8>)]) -> Vec<(SpoolIndex, &[u8])> {
        slices.iter().map(|(idx, data)| (*idx, data.as_slice())).collect()
    }

    fn test_encoder() -> BlobEncoder {
        BlobEncoder::try_new(SMALL).unwrap()
    }

    fn test_decoder() -> BlobDecoder {
        BlobDecoder::try_new(SMALL).unwrap()
    }

    #[test]
    fn test_roundtrip() {
        let original = vec![0xAB; 20_000];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let decoder = test_decoder();
        let recovered = decoder.decode(&refs(&slices)).unwrap();

        assert_eq!(recovered, original);
    }

    #[test]
    fn test_decode_with_only_data_slices() {
        let original = vec![0xCD; 15_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only exactly k (minimum required)
        let data_only: Vec<_> = slices.into_iter().take(k).collect();

        let decoder = test_decoder();
        let recovered = decoder.decode(&refs(&data_only)).unwrap();

        assert_eq!(recovered, original);
    }

    #[test]
    fn test_decode_with_missing_parity() {
        let original = vec![0xEF; 20_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only the first k (all data, no parity)
        let data_only: Vec<_> = slices.into_iter().take(k).collect();

        let decoder = test_decoder();
        let recovered = decoder.decode(&refs(&data_only)).unwrap();

        assert_eq!(recovered, original);
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

        let decoder = test_decoder();
        let recovered = decoder.decode(&refs(&scattered)).unwrap();

        assert_eq!(recovered, original);
    }

    #[test]
    fn test_decode_not_enough_slices() {
        let original = vec![0x34; 10_000];

        let mut encoder = test_encoder();
        let k = encoder.profile().k() as usize;
        let slices = encoder.encode(original).unwrap();

        // Only keep k - 1 slices (not enough)
        let too_few: Vec<_> = slices.into_iter().take(k - 1).collect();

        let decoder = test_decoder();
        let result = decoder.decode(&refs(&too_few));

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

        // Replace one slice's index with an invalid one (>= n)
        slices[0].0 = SpoolIndex::from(9999);

        let decoder = test_decoder();
        let result = decoder.decode(&refs(&slices));

        assert!(matches!(
            result,
            Err(DownloadError::InvalidSliceIndex(idx)) if idx == SpoolIndex::from(9999)
        ));
    }

    // an index a wider network would accept is refused at this network's n
    #[test]
    fn index_past_width() {
        let mut encoder = test_encoder();
        let mut slices: Vec<_> = encoder.encode(vec![0x77; 10_000]).unwrap();
        let width = u64::from(SMALL.n());

        slices[0].0 = SpoolIndex::from(width);

        let result = test_decoder().decode(&refs(&slices));

        assert!(matches!(
            result,
            Err(DownloadError::InvalidSliceIndex(idx)) if idx == SpoolIndex::from(width)
        ));
    }

    #[test]
    fn test_decode_empty_blob() {
        let original = vec![];

        let mut encoder = test_encoder();
        let slices = encoder.encode(original.clone()).unwrap();

        let decoder = test_decoder();
        let recovered = decoder.decode(&refs(&slices)).unwrap();

        assert_eq!(recovered, original);
    }

    #[test]
    fn profile_kept() {
        assert_eq!(test_decoder().profile(), SMALL);
    }

    // a profile no network can have builds no decoder
    #[test]
    fn bad_profile() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(0, 4));
        assert!(BlobDecoder::try_new(profile).is_err());
    }

    #[test]
    fn test_clay_roundtrip() {
        let original = vec![0xAB; 10_000];

        let mut encoder = BlobEncoder::try_new(TAPENET_PROFILE).unwrap();
        let decoder = BlobDecoder::try_new(TAPENET_PROFILE).unwrap();

        let slices = encoder.encode(original.clone()).unwrap();
        let recovered = decoder.decode(&refs(&slices)).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_clay_decode_with_missing_slices() {
        let original = vec![0xCD; 50_000];

        let mut encoder = BlobEncoder::try_new(TAPENET_PROFILE).unwrap();
        let decoder = BlobDecoder::try_new(TAPENET_PROFILE).unwrap();

        let slices = encoder.encode(original.clone()).unwrap();

        // Keep only first k slices (k=7 on tapenet)
        let k = encoder.profile().k() as usize;
        let partial: Vec<_> = slices.into_iter().take(k).collect();

        let recovered = decoder.decode(&refs(&partial)).unwrap();
        assert_eq!(original, recovered);
    }

    // one copy out of two recovers a mirrored blob
    #[test]
    fn mirror_one_copy() {
        let profile = ErasureCode::Replication(RepParams::new(2));
        let original = vec![0x3C; 10_000];

        let mut encoder = BlobEncoder::try_new(profile).unwrap();
        let decoder = BlobDecoder::try_new(profile).unwrap();

        let slices = encoder.encode(original.clone()).unwrap();
        let last: Vec<_> = slices.into_iter().skip(1).collect();

        assert_eq!(decoder.decode(&refs(&last)).unwrap(), original);
    }
}
