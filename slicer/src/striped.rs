//! Striped slicer for large blob encoding.
//!
//! Splits blobs into multiple stripes, encoding each stripe separately.
//! This bounds memory usage while handling arbitrarily large blobs.

use tape_core::encoding::EncodingProfile;

use crate::api::Slicer;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use crate::errors::{DecodeError, EncodeError};
use crate::codec::{StripedCodec, MappingStrategy, DEFAULT_STRIPE_SIZE};
use crate::types::{Blob, Slice};

/// A striped slicer that splits blobs into multiple stripes.
///
/// Each stripe is Clay-encoded into SPOOL_GROUP_SIZE shards. Shards are appended
/// to output slices using identity mapping (shard N -> slice N).
///
/// For fair load distribution across nodes, use `RotatedSlicer` instead.
pub struct StripedSlicer {
    codec: StripedCodec,
}

impl StripedSlicer {
    /// Create a new StripedSlicer with default Clay profile.
    pub fn new() -> Self {
        Self {
            codec: StripedCodec::new(DEFAULT_STRIPE_SIZE, MappingStrategy::Identity),
        }
    }

    /// Create with a specific encoding profile.
    pub fn with_profile(stripe_size: usize, profile: EncodingProfile) -> Self {
        Self {
            codec: StripedCodec::with_profile(stripe_size, MappingStrategy::Identity, profile),
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

    /// Get the current encoding profile.
    pub fn profile(&self) -> EncodingProfile {
        self.codec.profile()
    }
}

impl Default for StripedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for StripedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;

    fn k(&self) -> usize {
        self.profile().k() as usize
    }

    fn m(&self) -> usize {
        self.profile().m() as usize
    }

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError> {
        self.codec.encode_adaptive(blob)
    }

    fn decode(&mut self, slices: &[Option<Slice>; SPOOL_GROUP_SIZE]) -> Result<Blob, DecodeError> {
        self.codec.decode(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Slicer;

    // Default profile k=10, m=10
    const K: usize = 10;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SPOOL_GROUP_SIZE]) -> [Option<Slice>; SPOOL_GROUP_SIZE] {
        std::array::from_fn(|i| Some(slices[i].clone()))
    }

    fn keep_only(arr: &mut [Option<Slice>; SPOOL_GROUP_SIZE], keep: &[usize]) {
        let mut keep_set = vec![false; SPOOL_GROUP_SIZE];
        for &idx in keep {
            keep_set[idx] = true;
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
        let k = slicer.k();
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..k).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_with_missing_data_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let k = slicer.k();
        let payload = mk(2000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep some data slices (first 5) + all parity slices
        let mut keep_indices: Vec<usize> = (0..5).collect();
        keep_indices.extend(k..SPOOL_GROUP_SIZE);
        keep_only(&mut opt, &keep_indices);

        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= k);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let k = slicer.k();
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..(k - 1)).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SPOOL_GROUP_SIZE);
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

    #[test]
    fn test_k_m_from_slicer() {
        let slicer = StripedSlicer::default();
        assert_eq!(slicer.k(), K);
        assert_eq!(slicer.m(), K); // default is k=10, m=10
    }
}
