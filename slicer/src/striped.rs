//! Striped slicer for large blob encoding.
//!
//! Splits blobs into multiple stripes, encoding each stripe separately.
//! This bounds memory usage while handling arbitrarily large blobs.

use crate::api::Slicer;
use crate::consts::{PARITY_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::codec::{StripedCodec, MappingStrategy, DEFAULT_STRIPE_SIZE};
use crate::types::{Blob, Slice};

/// A striped slicer that splits blobs into multiple stripes.
///
/// Each stripe is Clay-encoded into SLICE_COUNT shards. Shards are appended
/// to output slices using identity mapping (shard N -> slice N).
///
/// For fair load distribution across nodes, use `RotatedSlicer` instead.
pub struct StripedSlicer {
    codec: StripedCodec,
}

impl StripedSlicer {
    /// Create a new StripedSlicer.
    pub fn new() -> Self {
        Self {
            codec: StripedCodec::new(DEFAULT_STRIPE_SIZE, MappingStrategy::Identity),
        }
    }

    /// Create with a specific initial stripe size (for testing).
    pub fn with_stripe_size(stripe_size: usize) -> Self {
        Self {
            codec: StripedCodec::new(stripe_size, MappingStrategy::Identity),
        }
    }

    /// Get the current stripe size.
    pub fn stripe_size(&self) -> usize {
        self.codec.stripe_size
    }
}

impl Default for StripedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for StripedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const PARITY_OUTPUT_SLICES: usize = PARITY_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        self.codec.encode_adaptive(blob)
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        self.codec.decode(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        std::array::from_fn(|i| Some(slices[i].clone()))
    }

    fn keep_only(arr: &mut [Option<Slice>; SLICE_COUNT], keep: &[usize]) {
        let mut keep_set = vec![false; SLICE_COUNT];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    #[test]
    fn test_stripe_size_constant() {
        assert_eq!(DEFAULT_STRIPE_SIZE, 10_000_000);
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(500);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_data_only() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_with_missing_data_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(2000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep some data slices (first 5) + all parity slices
        let mut keep_indices: Vec<usize> = (0..5).collect();
        keep_indices.extend(DATA_SLICES..SLICE_COUNT);
        keep_only(&mut opt, &keep_indices);

        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= DATA_SLICES);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_default_stripe_size() {
        let slicer = StripedSlicer::default();
        assert_eq!(slicer.stripe_size(), DEFAULT_STRIPE_SIZE);
    }
}
