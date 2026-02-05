//! Shared striping logic for StripedSlicer and RotatedSlicer.
//!
//! Both slicers split blobs into stripes and encode each stripe separately
//! using Clay codes (MSR erasure codes). The difference is how shards map
//! to output slices:
//! - StripedSlicer: identity mapping (shard N -> slice N)
//! - RotatedSlicer: rotated mapping for fair load distribution

use std::collections::HashMap;

use bytemuck::{Pod, Zeroable};
use clay_codes::ClayCode;
use tape_core::encoding::{ClayParams, EncodingProfile};

use crate::consts::SPOOL_GROUP_SIZE;
use crate::errors::{DecodeError, EncodeError};
use crate::slice_index::SliceIndex;
use crate::types::{Blob, Slice};

/// Default stripe size (10 MB).
pub const DEFAULT_STRIPE_SIZE: usize = 10_000_000;

/// Rotation step per stripe (coprime with SPOOL_GROUP_SIZE=20 for full coverage).
/// gcd(7, 20) = 1 ensures all positions are visited in 20 stripes.
pub const ROTATION_STEP: usize = 7;

/// Available stripe sizes for adaptive encoding.
/// Multiples of 2000 for Clay alignment (k × α × 2 = 10 × 100 × 2 = 2000).
pub const STRIPE_SIZES: [usize; 3] = [
    100_000,     // 100 KB
    1_000_000,   //   1 MB
    10_000_000,  //  10 MB
];

/// Select optimal stripe size based on blob size.
#[inline]
pub fn pick_stripe_size(blob_len: usize) -> usize {
    if blob_len <= 1_000_000 {
        STRIPE_SIZES[0] // 100 KB
    } else if blob_len <= 100_000_000 {
        STRIPE_SIZES[1] // 1 MB
    } else {
        STRIPE_SIZES[2] // 10 MB
    }
}

/// Metadata suffix appended to each slice.
///
/// Contains information needed to decode the blob:
/// - `version`: Format version for future extensibility
/// - `blob_len`: Original unencoded blob size in bytes
/// - `stripe_size`: Stripe size used during encoding
/// - `profile`: Encoding profile (type + params, 16 bytes)
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
}

impl SliceMetadata {
    pub const VERSION: u64 = 0;
    pub const SIZE: usize = std::mem::size_of::<Self>(); // 40 bytes

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
        let meta: Self = *bytemuck::from_bytes(suffix);

        if !STRIPE_SIZES.contains(&(meta.stripe_size as usize)) {
            return Err(DecodeError::InvalidLayout);
        }

        Ok(meta)
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn blob_len(&self) -> usize {
        self.blob_len as usize
    }

    pub fn stripe_size(&self) -> usize {
        self.stripe_size as usize
    }

    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }

    /// Get Clay params from profile (panics if not Clay encoding).
    pub fn clay_params(&self) -> ClayParams {
        self.profile.clay_params()
    }
}

/// Mapping strategy for shard-to-slice assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingStrategy {
    /// Identity mapping: shard N -> slice N (no rotation)
    Identity,
    /// Rotated mapping: shard N -> slice (N + stripe * ROTATION_STEP) % SPOOL_GROUP_SIZE
    Rotated,
}

/// Forward mapping: (stripe, shard) -> slice
#[inline]
pub fn shard_to_slice(strategy: MappingStrategy, stripe_idx: usize, shard_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => shard_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SPOOL_GROUP_SIZE;
            (shard_idx + offset) % SPOOL_GROUP_SIZE
        }
    }
}

/// Inverse mapping: (stripe, slice) -> shard
#[inline]
pub fn slice_to_shard(strategy: MappingStrategy, stripe_idx: usize, slice_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => slice_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SPOOL_GROUP_SIZE;
            (slice_idx + SPOOL_GROUP_SIZE - offset) % SPOOL_GROUP_SIZE
        }
    }
}

/// Round up `n` to be divisible by `divisor`.
#[inline]
pub fn round_up_to(n: usize, divisor: usize) -> usize {
    ((n + divisor - 1) / divisor) * divisor
}

/// Core striped encoder/decoder with configurable mapping strategy.
/// Uses Clay codes (MSR erasure codes) for encoding/decoding.
pub struct StripedCodec {
    pub stripe_size: usize,
    pub strategy: MappingStrategy,
    clay: ClayCode,
    /// Encoding profile (type + params).
    profile: EncodingProfile,
    /// Number of total slices (n = k + m).
    n: u8,
    /// Number of data slices.
    k: u8,
    /// Number of parity slices.
    m: u8,
    /// Clay helper count (d).
    d: u8,
}

impl StripedCodec {
    /// Create a new codec with the given stripe size and mapping strategy.
    /// Uses the default Clay profile (k=10, m=10, d=19).
    pub fn new(stripe_size: usize, strategy: MappingStrategy) -> Self {
        Self::with_profile(stripe_size, strategy, EncodingProfile::clay_default())
    }

    /// Create a new codec with a specific encoding profile.
    pub fn with_profile(stripe_size: usize, strategy: MappingStrategy, profile: EncodingProfile) -> Self {
        assert!(stripe_size > 0, "stripe_size must be > 0");

        let cp = profile.clay_params();
        let n = cp.n();
        let k = cp.k();
        let m = cp.m();
        let d = cp.d();

        let clay = ClayCode::new(k as usize, m as usize, d as usize)
            .expect("Clay code init");

        Self {
            stripe_size,
            strategy,
            clay,
            profile,
            n,
            k,
            m,
            d,
        }
    }

    /// Get the current encoding profile.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }

    /// Reconfigure the codec for a different stripe size and/or profile.
    fn reconfigure(&mut self, stripe_size: usize, profile: EncodingProfile) {
        self.stripe_size = stripe_size;

        // Only recreate Clay code if profile changed
        if self.profile != profile {
            let cp = profile.clay_params();
            self.n = cp.n();
            self.k = cp.k();
            self.m = cp.m();
            self.d = cp.d();
            self.profile = profile;

            self.clay = ClayCode::new(self.k as usize, self.m as usize, self.d as usize)
                .expect("Clay code init");
        }
    }

    /// Encode with automatically selected stripe size based on blob length.
    pub fn encode_adaptive(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError> {
        let optimal_stripe = pick_stripe_size(blob.len());

        if self.stripe_size != optimal_stripe {
            self.reconfigure(optimal_stripe, self.profile);
        }

        self.encode(blob)
    }

    /// Encode a blob into SPOOL_GROUP_SIZE slices.
    pub fn encode(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();

        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;

        // Encode first stripe to determine chunk size (Clay handles padding internally)
        let first_stripe_data = &data[..self.stripe_size.min(blob_len)];
        let first_chunks = self.clay.encode(first_stripe_data);
        let chunk_size = first_chunks[0].len();

        // Initialize output slices
        let mut slices: Vec<Vec<u8>> = (0..SPOOL_GROUP_SIZE)
            .map(|_| Vec::with_capacity(num_stripes * chunk_size + SliceMetadata::SIZE))
            .collect();

        // Distribute first stripe chunks
        for (shard_idx, chunk) in first_chunks.iter().enumerate() {
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx].extend_from_slice(chunk);
        }

        // Encode remaining stripes
        for s in 1..num_stripes {
            let start = s * self.stripe_size;
            let end = (start + self.stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            let chunks = self.clay.encode(stripe_data);

            // All chunks must be the same size as first stripe's chunks
            // (Clay pads internally; for the last shorter stripe, chunk_size may differ)
            // We pad the stripe to self.stripe_size to ensure consistent chunk sizes.
            let chunks = if chunks[0].len() != chunk_size {
                let mut padded = stripe_data.to_vec();
                padded.resize(self.stripe_size, 0);
                self.clay.encode(&padded)
            } else {
                chunks
            };

            for (shard_idx, chunk) in chunks.iter().enumerate() {
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                slices[slice_idx].extend_from_slice(chunk);
            }
        }

        // Append metadata with current profile
        let metadata = SliceMetadata::with_profile(blob_len, self.stripe_size, self.profile);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SPOOL_GROUP_SIZE slices"))
    }

    /// Decode slices back into the original blob.
    pub fn decode(&mut self, slices: &[Option<Slice>; SPOOL_GROUP_SIZE]) -> Result<Blob, DecodeError> {
        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;

        let metadata = SliceMetadata::from_slice(&sample.data)?;

        // Check minimum slices using profile's k value
        let min_slices = metadata.clay_params().k() as usize;
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < min_slices {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Reconfigure codec if stripe size or profile differs
        if self.stripe_size != metadata.stripe_size() || self.profile != metadata.profile() {
            self.reconfigure(metadata.stripe_size(), metadata.profile());
        }

        let blob_len = metadata.blob_len();

        if blob_len == 0 {
            return Ok(Blob::from(Vec::new()));
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;

        // Determine chunk_size from a sample slice: (total_len - metadata) / num_stripes
        let total_data_len = sample.data.len() - SliceMetadata::SIZE;
        if total_data_len == 0 || total_data_len % num_stripes != 0 {
            return Err(DecodeError::InvalidLayout);
        }
        let chunk_size = total_data_len / num_stripes;

        let expected_slice_len = num_stripes * chunk_size + SliceMetadata::SIZE;
        for slice in slices.iter().flatten() {
            if slice.data.len() != expected_slice_len {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let mut output = Vec::with_capacity(blob_len);

        for s in 0..num_stripes {
            let chunk_offset = s * chunk_size;

            // Build available map and erasures list for Clay decode
            let mut available: HashMap<usize, Vec<u8>> = HashMap::new();
            let mut erasures: Vec<usize> = Vec::new();

            for shard_idx in 0..SPOOL_GROUP_SIZE {
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                match &slices[slice_idx] {
                    Some(slice) => {
                        let chunk = slice.data[chunk_offset..chunk_offset + chunk_size].to_vec();
                        available.insert(shard_idx, chunk);
                    }
                    None => {
                        erasures.push(shard_idx);
                    }
                }
            }

            let stripe_data = self
                .clay
                .decode(&available, &erasures)
                .map_err(|_| DecodeError::BadEncoding)?;

            // Clay decode returns the full padded data (k * chunk_size).
            // Take only what we need for this stripe.
            let take = if s == num_stripes - 1 {
                blob_len - output.len()
            } else {
                self.stripe_size
            };

            if take > stripe_data.len() {
                return Err(DecodeError::InvalidLayout);
            }
            output.extend_from_slice(&stripe_data[..take]);
        }

        Ok(Blob::from(output))
    }

    fn encode_empty_blob(&mut self) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError> {
        let empty = vec![0u8; self.stripe_size];
        let chunks = self.clay.encode(&empty);
        let chunk_size = chunks[0].len();

        let mut slices: Vec<Vec<u8>> = vec![Vec::with_capacity(chunk_size + SliceMetadata::SIZE); SPOOL_GROUP_SIZE];

        for (shard_idx, chunk) in chunks.iter().enumerate() {
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx] = chunk.clone();
        }

        // Append metadata (blob_len = 0 for empty blob) with current profile
        let metadata = SliceMetadata::with_profile(0, self.stripe_size, self.profile);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SPOOL_GROUP_SIZE slices"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_mapping() {
        for stripe in 0..10 {
            for shard in 0..SPOOL_GROUP_SIZE {
                let slice = shard_to_slice(MappingStrategy::Identity, stripe, shard);
                assert_eq!(slice, shard);
                let recovered = slice_to_shard(MappingStrategy::Identity, stripe, slice);
                assert_eq!(recovered, shard);
            }
        }
    }

    #[test]
    fn test_rotated_mapping_inverse() {
        for stripe in 0..10 {
            for shard in 0..SPOOL_GROUP_SIZE {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_rotation_step_coprime() {
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, SPOOL_GROUP_SIZE), 1);
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 100;
        let mut slice_hits = vec![0usize; SPOOL_GROUP_SIZE];

        for stripe in 0..num_stripes {
            for shard in 0..SPOOL_GROUP_SIZE {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        // Each slice should be hit equally
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, num_stripes, "slice {} hit count mismatch", i);
        }
    }

    #[test]
    fn test_pick_stripe_size() {
        assert_eq!(pick_stripe_size(100), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(1_000_000), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(1_000_001), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(100_000_000), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(100_000_001), STRIPE_SIZES[2]);
    }
}
