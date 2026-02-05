//! Striped erasure coder with optional rotation.
//!
//! `StripedCoder<C>` wraps any `Slicer` implementation and adds:
//! - Stripe splitting (adaptive size selection for optimal encoding)
//! - Metadata suffix (blob_len, stripe_size, profile for decoding)
//! - Optional rotation mapping for fair load distribution

use std::collections::HashSet;

use bytemuck::{Pod, Zeroable};
use tape_core::encoding::EncodingProfile;

use crate::clay::ClayCoder;
use crate::errors::{DecodeError, EncodeError};
use crate::Slicer;

/// Rotation step per stripe (coprime with n=20 for full coverage).
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
}

/// Mapping strategy for shard-to-slice assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingStrategy {
    /// Identity mapping: shard N -> slice N (no rotation)
    Identity,
    /// Rotated mapping: shard N -> slice (N + stripe * ROTATION_STEP) % n
    Rotated,
}

/// Forward mapping: (stripe, shard) -> slice
#[inline]
fn shard_to_slice(strategy: MappingStrategy, n: usize, stripe_idx: usize, shard_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => shard_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % n;
            (shard_idx + offset) % n
        }
    }
}

/// Inverse mapping: (stripe, slice) -> shard
#[inline]
#[allow(dead_code)]
fn slice_to_shard(strategy: MappingStrategy, n: usize, stripe_idx: usize, slice_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => slice_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % n;
            (slice_idx + n - offset) % n
        }
    }
}

/// Striped erasure coder that wraps any `Slicer` implementation.
///
/// Adds striping (splits blobs into multiple stripes), metadata (for decoding),
/// and optional rotation (for fair load distribution).
///
/// # Type Parameters
/// * `C` - The underlying coder implementing `Slicer` (e.g., `ClayCoder`)
///
/// # Examples
/// ```ignore
/// // Production: striped + rotated Clay codes
/// let mut slicer = StripedCoder::with_rotation(ClayCoder::new(10, 10, 19));
/// let chunks = slicer.encode(&data)?;
///
/// // Striped only (no rotation)
/// let mut slicer = StripedCoder::new(ClayCoder::new(10, 10, 19));
/// ```
pub struct StripedCoder<C: Slicer> {
    coder: C,
    stripe_size: usize,
    strategy: MappingStrategy,
    profile: EncodingProfile,
}

impl<C: Slicer> StripedCoder<C> {
    /// Create a new striped coder with identity mapping (no rotation).
    ///
    /// Uses default stripe size (10 MB) and Clay default profile.
    pub fn new(coder: C) -> Self {
        Self {
            coder,
            stripe_size: STRIPE_SIZES[2],
            strategy: MappingStrategy::Identity,
            profile: EncodingProfile::clay_default(),
        }
    }

    /// Create a new striped coder with rotation (production mode).
    ///
    /// Rotation ensures fair load distribution across all nodes.
    pub fn with_rotation(coder: C) -> Self {
        Self {
            coder,
            stripe_size: STRIPE_SIZES[2],
            strategy: MappingStrategy::Rotated,
            profile: EncodingProfile::clay_default(),
        }
    }

    /// Create with a specific stripe size.
    pub fn with_stripe_size(coder: C, stripe_size: usize) -> Self {
        Self {
            coder,
            stripe_size,
            strategy: MappingStrategy::Identity,
            profile: EncodingProfile::clay_default(),
        }
    }

    /// Create with a specific encoding profile and rotation.
    pub fn with_profile(coder: C, stripe_size: usize, rotated: bool, profile: EncodingProfile) -> Self {
        Self {
            coder,
            stripe_size,
            strategy: if rotated { MappingStrategy::Rotated } else { MappingStrategy::Identity },
            profile,
        }
    }

    /// Get the current stripe size.
    pub fn stripe_size(&self) -> usize {
        self.stripe_size
    }

    /// Get the current encoding profile.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }

    /// Get the mapping strategy.
    pub fn strategy(&self) -> MappingStrategy {
        self.strategy
    }

    /// Reconfigure the coder for a different stripe size.
    fn set_stripe_size(&mut self, stripe_size: usize) {
        self.stripe_size = stripe_size;
    }
}

impl StripedCoder<ClayCoder> {
    /// Create a new striped Clay coder with rotation (production default).
    ///
    /// Uses default Clay parameters (k=10, m=10, d=19).
    pub fn clay_default() -> Self {
        Self::with_rotation(ClayCoder::new(10, 10, 19))
    }

    /// Reconfigure the underlying Clay coder for a different profile.
    pub fn reconfigure_clay(&mut self, profile: EncodingProfile) {
        if self.profile != profile {
            self.profile = profile;
            self.coder = ClayCoder::from_params(profile.clay_params());
        }
    }
}

impl<C: Slicer> Slicer for StripedCoder<C> {
    fn k(&self) -> usize {
        self.coder.k()
    }

    fn m(&self) -> usize {
        self.coder.m()
    }

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        let blob_len = data.len();

        // Select optimal stripe size
        let optimal_stripe = pick_stripe_size(blob_len);
        if self.stripe_size != optimal_stripe {
            self.set_stripe_size(optimal_stripe);
        }

        // Handle empty blob
        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let n = self.n();
        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;

        // Encode first stripe to determine chunk size
        let first_stripe_data = &data[..self.stripe_size.min(blob_len)];
        let first_chunks = self.coder.encode(first_stripe_data)?;
        let chunk_size = first_chunks[0].len();

        // Initialize output slices
        let mut slices: Vec<Vec<u8>> = (0..n)
            .map(|_| Vec::with_capacity(num_stripes * chunk_size + SliceMetadata::SIZE))
            .collect();

        // Distribute first stripe chunks
        for (shard_idx, chunk) in first_chunks.iter().enumerate() {
            let slice_idx = shard_to_slice(self.strategy, n, 0, shard_idx);
            slices[slice_idx].extend_from_slice(chunk);
        }

        // Encode remaining stripes
        for s in 1..num_stripes {
            let start = s * self.stripe_size;
            let end = (start + self.stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            let chunks = self.coder.encode(stripe_data)?;

            // Ensure consistent chunk sizes across stripes
            let chunks = if chunks[0].len() != chunk_size {
                // Pad the last stripe to full size for consistent chunks
                let mut padded = stripe_data.to_vec();
                padded.resize(self.stripe_size, 0);
                self.coder.encode(&padded)?
            } else {
                chunks
            };

            for (shard_idx, chunk) in chunks.iter().enumerate() {
                let slice_idx = shard_to_slice(self.strategy, n, s, shard_idx);
                slices[slice_idx].extend_from_slice(chunk);
            }
        }

        // Append metadata
        let metadata = SliceMetadata::with_profile(blob_len, self.stripe_size, self.profile);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        Ok(slices)
    }

    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.is_empty() {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Parse metadata from any available chunk
        let sample_data = chunks[0].1;
        let metadata = SliceMetadata::from_slice(sample_data)?;

        // Check minimum chunks using profile's k value
        let min_chunks = metadata.profile().clay_params().k() as usize;
        if chunks.len() < min_chunks {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Reconfigure if needed
        if self.stripe_size != metadata.stripe_size() {
            self.stripe_size = metadata.stripe_size();
        }

        let blob_len = metadata.blob_len();
        if blob_len == 0 {
            return Ok(Vec::new());
        }

        let n = self.n();
        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;

        // Determine chunk_size from sample: (total_len - metadata) / num_stripes
        let total_data_len = sample_data.len() - SliceMetadata::SIZE;
        if total_data_len == 0 || total_data_len % num_stripes != 0 {
            return Err(DecodeError::InvalidLayout);
        }
        let chunk_size = total_data_len / num_stripes;

        // Validate all chunks have expected size
        let expected_slice_len = num_stripes * chunk_size + SliceMetadata::SIZE;
        for &(_, data) in chunks {
            if data.len() != expected_slice_len {
                return Err(DecodeError::InvalidLayout);
            }
        }

        // Build index set for quick lookup
        let present_indices: HashSet<usize> = chunks.iter().map(|&(i, _)| i).collect();
        let chunks_map: std::collections::HashMap<usize, &[u8]> =
            chunks.iter().map(|&(i, d)| (i, d)).collect();

        let mut output = Vec::with_capacity(blob_len);

        for s in 0..num_stripes {
            let chunk_offset = s * chunk_size;

            // Build available chunks and erasures for this stripe
            let mut stripe_chunks: Vec<(usize, &[u8])> = Vec::with_capacity(chunks.len());

            for shard_idx in 0..n {
                let slice_idx = shard_to_slice(self.strategy, n, s, shard_idx);
                if present_indices.contains(&slice_idx) {
                    let slice_data = chunks_map[&slice_idx];
                    let chunk = &slice_data[chunk_offset..chunk_offset + chunk_size];
                    stripe_chunks.push((shard_idx, chunk));
                }
            }

            let stripe_data = self.coder.decode(&stripe_chunks)?;

            // Take only what we need for this stripe
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

        Ok(output)
    }
}

impl<C: Slicer> StripedCoder<C> {
    fn encode_empty_blob(&mut self) -> Result<Vec<Vec<u8>>, EncodeError> {
        let n = self.n();

        // Encode a full stripe of zeros
        let empty = vec![0u8; self.stripe_size];
        let chunks = self.coder.encode(&empty)?;
        let chunk_size = chunks[0].len();

        let mut slices: Vec<Vec<u8>> = vec![Vec::with_capacity(chunk_size + SliceMetadata::SIZE); n];

        for (shard_idx, chunk) in chunks.iter().enumerate() {
            let slice_idx = shard_to_slice(self.strategy, n, 0, shard_idx);
            slices[slice_idx] = chunk.clone();
        }

        // Append metadata (blob_len = 0 for empty blob)
        let metadata = SliceMetadata::with_profile(0, self.stripe_size, self.profile);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        Ok(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClayCoder;

    const N: usize = 20; // k=10 + m=10

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_refs(chunks: &[Vec<u8>]) -> Vec<(usize, &[u8])> {
        chunks.iter().enumerate().map(|(i, c)| (i, c.as_slice())).collect()
    }

    fn keep_only<'a>(chunks: &'a [Vec<u8>], keep: &[usize]) -> Vec<(usize, &'a [u8])> {
        chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| keep.contains(i))
            .map(|(i, c)| (i, c.as_slice()))
            .collect()
    }

    #[test]
    fn test_identity_mapping() {
        for stripe in 0..10 {
            for shard in 0..N {
                let slice = shard_to_slice(MappingStrategy::Identity, N, stripe, shard);
                assert_eq!(slice, shard);
                let recovered = slice_to_shard(MappingStrategy::Identity, N, stripe, slice);
                assert_eq!(recovered, shard);
            }
        }
    }

    #[test]
    fn test_rotated_mapping_inverse() {
        for stripe in 0..10 {
            for shard in 0..N {
                let slice = shard_to_slice(MappingStrategy::Rotated, N, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, N, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_rotation_step_coprime() {
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, N), 1);
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 100;
        let mut slice_hits = vec![0usize; N];

        for stripe in 0..num_stripes {
            for shard in 0..N {
                let slice = shard_to_slice(MappingStrategy::Rotated, N, stripe, shard);
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

    #[test]
    fn test_roundtrip_small_identity() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let payload = mk(500);
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_roundtrip_small_rotated() {
        let mut slicer = StripedCoder::with_profile(
            ClayCoder::new(10, 10, 19),
            1024,
            true,
            EncodingProfile::clay_default(),
        );
        let payload = mk(500);
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let payload = Vec::new();
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_decode_data_only() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let k = slicer.k();
        let payload = mk(3000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..k).collect::<Vec<_>>());
        let restored = slicer.decode(&partial).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_decode_with_missing_slices() {
        let mut slicer = StripedCoder::with_profile(
            ClayCoder::new(10, 10, 19),
            1024,
            true,
            EncodingProfile::clay_default(),
        );
        let k = slicer.k();
        let payload = mk(3000);
        let chunks = slicer.encode(&payload).unwrap();

        // Keep exactly k slices (first k)
        let partial = keep_only(&chunks, &(0..k).collect::<Vec<_>>());
        assert_eq!(partial.len(), k);

        let restored = slicer.decode(&partial).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let k = slicer.k();
        let payload = mk(1000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..(k - 1)).collect::<Vec<_>>());
        let res = slicer.decode(&partial);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();
        let first_len = chunks[0].len();
        for chunk in &chunks {
            assert_eq!(chunk.len(), first_len);
        }
    }

    #[test]
    fn test_clay_default_constructor() {
        let mut slicer = StripedCoder::clay_default();
        assert_eq!(slicer.k(), 10);
        assert_eq!(slicer.m(), 10);
        assert_eq!(slicer.strategy(), MappingStrategy::Rotated);

        let payload = mk(1000);
        let chunks = slicer.encode(&payload).unwrap();
        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_k_m_from_slicer() {
        let slicer = StripedCoder::new(ClayCoder::new(10, 10, 19));
        assert_eq!(slicer.k(), 10);
        assert_eq!(slicer.m(), 10);
        assert_eq!(slicer.n(), 20);
    }

    #[test]
    fn test_metadata_parsing() {
        let mut slicer = StripedCoder::with_stripe_size(ClayCoder::new(10, 10, 19), 1024);
        let payload = mk(2000);
        let chunks = slicer.encode(&payload).unwrap();

        // Parse metadata from first chunk
        let meta = SliceMetadata::from_slice(&chunks[0]).unwrap();
        assert_eq!(meta.blob_len(), 2000);
        assert!(STRIPE_SIZES.contains(&meta.stripe_size()));
    }
}
