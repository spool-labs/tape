//! Rotated slicer for fair load distribution.
//!
//! Extends striped encoding with per-stripe rotation to ensure all nodes
//! receive approximately equal amounts of data and parity chunks over time.

use tape_core::encoding::EncodingProfile;

use crate::api::Slicer;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use crate::errors::{DecodeError, EncodeError};
use crate::codec::{StripedCodec, MappingStrategy, DEFAULT_STRIPE_SIZE};
use crate::types::{Blob, Slice};

/// A rotated slicer that extends striped encoding with per-stripe rotation.
///
/// This provides fair load distribution across all nodes by rotating
/// the shard-to-slice mapping for each stripe. Over many stripes, each node
/// receives approximately equal amounts of data and parity chunks.
///
/// The rotation step is coprime with SPOOL_GROUP_SIZE, ensuring full coverage
/// of all slices.
pub struct RotatedSlicer {
    codec: StripedCodec,
}

impl RotatedSlicer {
    /// Create a new RotatedSlicer with default Clay profile.
    pub fn new() -> Self {
        Self {
            codec: StripedCodec::new(DEFAULT_STRIPE_SIZE, MappingStrategy::Rotated),
        }
    }

    /// Create with a specific encoding profile.
    pub fn with_profile(stripe_size: usize, profile: EncodingProfile) -> Self {
        Self {
            codec: StripedCodec::with_profile(stripe_size, MappingStrategy::Rotated, profile),
        }
    }

    /// Create with a specific initial stripe size (for testing).
    pub fn with_stripe_size(stripe_size: usize) -> Self {
        Self {
            codec: StripedCodec::new(stripe_size, MappingStrategy::Rotated),
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

impl Default for RotatedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for RotatedSlicer {
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
    use crate::codec::{shard_to_slice, slice_to_shard, ROTATION_STEP};

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
    fn test_rotation_step() {
        assert_eq!(ROTATION_STEP, 7);
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, SPOOL_GROUP_SIZE), 1);
    }

    #[test]
    fn test_rotation_inverse() {
        for stripe in 0..10 {
            for shard in 0..SPOOL_GROUP_SIZE {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(500);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_with_missing_slices() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let k = slicer.k();
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep exactly k slices (first k)
        let keep_indices: Vec<usize> = (0..k).collect();
        keep_only(&mut opt, &keep_indices);

        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= k);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
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
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SPOOL_GROUP_SIZE);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_default_stripe_size() {
        let slicer = RotatedSlicer::default();
        assert_eq!(slicer.stripe_size(), DEFAULT_STRIPE_SIZE);
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 1024;
        let mut slice_hits = vec![0usize; SPOOL_GROUP_SIZE];

        for stripe in 0..num_stripes {
            for shard in 0..SPOOL_GROUP_SIZE {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        let expected_hits_per_slice = num_stripes;
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, expected_hits_per_slice, "slice {} mismatch", i);
        }
    }

    #[test]
    fn test_k_m_from_slicer() {
        let slicer = RotatedSlicer::default();
        assert_eq!(slicer.k(), 10);
        assert_eq!(slicer.m(), 10);
    }
}
