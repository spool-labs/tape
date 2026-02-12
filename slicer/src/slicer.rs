//! Striped erasure coder with optional rotation.
//!
//! `Slicer<C>` wraps any `ErasureCoder` implementation and adds:
//! - Stripe splitting (adaptive size selection for optimal encoding)
//! - Metadata suffix (blob_len, stripe_size, profile for decoding)
//! - Optional rotation mapping for fair load distribution

use std::collections::HashSet;

use tape_core::encoding::{ClayParams, EncodingProfile};

use crate::adaptive::{pick_stripe_size, DEFAULT_STRIPE_SIZE};
use crate::clay::ClayCoder;
use crate::errors::{DecodeError, EncodeError};
use crate::metadata::SliceMetadata;
use crate::ErasureCoder;

/// Rotation step per stripe (coprime with n=20 for full coverage).
/// gcd(7, 20) = 1 ensures all positions are visited in 20 stripes.
pub const ROTATION_STEP: usize = 7;

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
pub fn shard_to_slice(strategy: MappingStrategy, n: usize, stripe_idx: usize, shard_idx: usize) -> usize {
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
pub fn slice_to_shard(strategy: MappingStrategy, n: usize, stripe_idx: usize, slice_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => slice_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % n;
            (slice_idx + n - offset) % n
        }
    }
}

/// Distribute encoded chunks to output slices using rotation mapping.
///
/// Each chunk from the coder is placed into the appropriate slice based on
/// the mapping strategy and current stripe index.
fn distribute_chunks(
    strategy: MappingStrategy,
    n: usize,
    stripe_idx: usize,
    chunks: &[Vec<u8>],
    slices: &mut [Vec<u8>],
) {
    for (shard_idx, chunk) in chunks.iter().enumerate() {
        let slice_idx = shard_to_slice(strategy, n, stripe_idx, shard_idx);
        slices[slice_idx].extend_from_slice(chunk);
    }
}

/// Validate slice layout and compute stripe parameters.
///
/// Checks that all provided chunks have consistent sizes and computes
/// the number of stripes and per-stripe chunk size from the metadata.
///
/// Returns (num_stripes, chunk_size) on success.
fn validate_layout(
    chunks: &[(usize, &[u8])],
    metadata: &SliceMetadata,
) -> Result<(usize, usize), DecodeError> {
    let blob_len = metadata.blob_len();
    let stripe_size = metadata.stripe_size();
    let num_stripes = (blob_len + stripe_size - 1) / stripe_size;

    // Determine chunk_size from first sample: (total_len - metadata) / num_stripes
    let sample_len = chunks[0].1.len();
    let total_data_len = sample_len.saturating_sub(SliceMetadata::SIZE);

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

    Ok((num_stripes, chunk_size))
}

/// Striped erasure coder that wraps any `ErasureCoder` implementation.
///
/// Adds striping (splits blobs into multiple stripes), metadata (for decoding),
/// and optional rotation (for fair load distribution).
///
/// # Type Parameters
/// * `C` - The underlying coder implementing `ErasureCoder` (e.g., `ClayCoder`)
///
/// # Examples
/// ```ignore
/// // Production: striped + rotated Clay codes
/// let mut slicer = Slicer::with_rotation(ClayCoder::new(20, 10, 19));
/// let chunks = slicer.encode(&data)?;
///
/// // Striped only (no rotation)
/// let mut slicer = Slicer::new(ClayCoder::new(20, 10, 19));
/// ```
pub struct Slicer<C: ErasureCoder> {
    pub coder: C,
    pub stripe_size: usize,
    pub strategy: MappingStrategy,
    pub profile: EncodingProfile,
    /// Chunk/group index embedded in slice metadata. Ensures that identical
    /// data chunks at different positions produce distinct commitments.
    pub chunk_index: u64,
}

impl<C: ErasureCoder> Slicer<C> {
    /// Create a new striped coder with identity mapping (no rotation).
    ///
    /// Uses default stripe size (10 MB) and Clay default profile.
    pub fn new(coder: C) -> Self {
        Self {
            coder,
            stripe_size: DEFAULT_STRIPE_SIZE,
            strategy: MappingStrategy::Identity,
            profile: EncodingProfile::clay_default(),
            chunk_index: 0,
        }
    }

    /// Create a new striped coder with rotation (production mode).
    ///
    /// Rotation ensures fair load distribution across all nodes.
    pub fn with_rotation(coder: C) -> Self {
        Self {
            coder,
            stripe_size: DEFAULT_STRIPE_SIZE,
            strategy: MappingStrategy::Rotated,
            profile: EncodingProfile::clay_default(),
            chunk_index: 0,
        }
    }

    /// Create with a specific stripe size.
    pub fn with_stripe_size(coder: C, stripe_size: usize) -> Self {
        Self {
            coder,
            stripe_size,
            strategy: MappingStrategy::Identity,
            profile: EncodingProfile::clay_default(),
            chunk_index: 0,
        }
    }

    /// Create with a specific encoding profile and rotation.
    pub fn with_profile(coder: C, stripe_size: usize, rotated: bool, profile: EncodingProfile) -> Self {
        Self {
            coder,
            stripe_size,
            strategy: if rotated { MappingStrategy::Rotated } else { MappingStrategy::Identity },
            profile,
            chunk_index: 0,
        }
    }

    /// Set the chunk index for metadata. Ensures identical data at different
    /// positions produces distinct slice commitments.
    pub fn set_chunk_index(&mut self, index: u64) {
        self.chunk_index = index;
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

impl Slicer<ClayCoder> {
    /// Create a new striped Clay coder with rotation (production default).
    ///
    /// Uses default Clay parameters (k=7, m=13, d=16).
    pub fn clay_default() -> Self {
        Self::with_rotation(ClayCoder::from_params(ClayParams::default()))
    }

    /// Reconfigure the underlying Clay coder for a different profile.
    pub fn reconfigure_clay(&mut self, profile: EncodingProfile) {
        if self.profile != profile {
            self.profile = profile;
            self.coder = ClayCoder::from_params(profile.clay_params());
        }
    }

}

impl<C: ErasureCoder> ErasureCoder for Slicer<C> {
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
        distribute_chunks(self.strategy, n, 0, &first_chunks, &mut slices);

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

            distribute_chunks(self.strategy, n, s, &chunks, &mut slices);
        }

        // Append metadata (includes chunk_index for position-dependent commitment)
        let mut metadata = SliceMetadata::with_profile(blob_len, self.stripe_size, self.profile);
        metadata.chunk_index = self.chunk_index;
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
        let (num_stripes, chunk_size) = validate_layout(chunks, &metadata)?;

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

impl<C: ErasureCoder> Slicer<C> {
    fn encode_empty_blob(&mut self) -> Result<Vec<Vec<u8>>, EncodeError> {
        let n = self.n();

        // Encode a full stripe of zeros
        let empty = vec![0u8; self.stripe_size];
        let chunks = self.coder.encode(&empty)?;
        let chunk_size = chunks[0].len();

        let mut slices: Vec<Vec<u8>> = vec![Vec::with_capacity(chunk_size + SliceMetadata::SIZE); n];
        distribute_chunks(self.strategy, n, 0, &chunks, &mut slices);

        // Append metadata (blob_len = 0 for empty blob)
        let mut metadata = SliceMetadata::with_profile(0, self.stripe_size, self.profile);
        metadata.chunk_index = self.chunk_index;
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        Ok(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClayCoder, STRIPE_SIZES};

    const N: usize = 20; // k=7 + m=13 (default Clay)

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
    fn test_identity() {
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
    fn test_rotated_inverse() {
        for stripe in 0..10 {
            for shard in 0..N {
                let slice = shard_to_slice(MappingStrategy::Rotated, N, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, N, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_step_coprime() {
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, N), 1);
    }

    #[test]
    fn test_distribution() {
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
    fn test_stripe_size() {
        assert_eq!(pick_stripe_size(100), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(1_000_000), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(1_000_001), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(100_000_000), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(100_000_001), DEFAULT_STRIPE_SIZE);
    }

    #[test]
    fn test_small_identity() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = mk(500);
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_small_rotated() {
        let mut slicer = Slicer::with_profile(
            ClayCoder::new(20, 10, 19),
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
    fn test_multi_stripe() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_empty() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = Vec::new();
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_data_only() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let k = slicer.k();
        let payload = mk(3000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..k).collect::<Vec<_>>());
        let restored = slicer.decode(&partial).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_missing_slices() {
        let mut slicer = Slicer::with_profile(
            ClayCoder::new(20, 10, 19),
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
    fn test_insufficient() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let k = slicer.k();
        let payload = mk(1000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..(k - 1)).collect::<Vec<_>>());
        let res = slicer.decode(&partial);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_uniform_slices() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();
        let first_len = chunks[0].len();
        for chunk in &chunks {
            assert_eq!(chunk.len(), first_len);
        }
    }

    #[test]
    fn test_clay_default() {
        let mut slicer = Slicer::clay_default();
        assert_eq!(slicer.k(), 7);
        assert_eq!(slicer.m(), 13);
        assert_eq!(slicer.strategy(), MappingStrategy::Rotated);

        let payload = mk(1000);
        let chunks = slicer.encode(&payload).unwrap();
        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_accessors() {
        let slicer = Slicer::new(ClayCoder::new(20, 10, 19));
        assert_eq!(slicer.k(), 10);
        assert_eq!(slicer.m(), 10);
        assert_eq!(slicer.n(), 20);
    }

    #[test]
    fn test_metadata() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = mk(2000);
        let chunks = slicer.encode(&payload).unwrap();

        // Parse metadata from first chunk
        let meta = SliceMetadata::from_slice(&chunks[0]).unwrap();
        assert_eq!(meta.blob_len(), 2000);
        assert!(STRIPE_SIZES.contains(&meta.stripe_size()));
    }

    #[test]
    fn test_distribute_identity() {
        let n = 4;
        let chunks: Vec<Vec<u8>> = vec![
            vec![1, 2],
            vec![3, 4],
            vec![5, 6],
            vec![7, 8],
        ];
        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); n];

        distribute_chunks(MappingStrategy::Identity, n, 0, &chunks, &mut slices);

        // Identity: chunk i goes to slice i
        assert_eq!(slices[0], vec![1, 2]);
        assert_eq!(slices[1], vec![3, 4]);
        assert_eq!(slices[2], vec![5, 6]);
        assert_eq!(slices[3], vec![7, 8]);
    }

    #[test]
    fn test_distribute_rotated() {
        let n = 4;
        let chunks: Vec<Vec<u8>> = vec![
            vec![1, 2],
            vec![3, 4],
            vec![5, 6],
            vec![7, 8],
        ];

        // Stripe 0: offset = (0 * 7) % 4 = 0, so same as identity
        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); n];
        distribute_chunks(MappingStrategy::Rotated, n, 0, &chunks, &mut slices);
        assert_eq!(slices[0], vec![1, 2]);
        assert_eq!(slices[1], vec![3, 4]);

        // Stripe 1: offset = (1 * 7) % 4 = 3
        // shard 0 -> slice (0 + 3) % 4 = 3
        // shard 1 -> slice (1 + 3) % 4 = 0
        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); n];
        distribute_chunks(MappingStrategy::Rotated, n, 1, &chunks, &mut slices);
        assert_eq!(slices[3], vec![1, 2]); // shard 0
        assert_eq!(slices[0], vec![3, 4]); // shard 1
    }

    #[test]
    fn test_distribute_accum() {
        let n = 2;
        let chunks1: Vec<Vec<u8>> = vec![vec![1], vec![2]];
        let chunks2: Vec<Vec<u8>> = vec![vec![3], vec![4]];

        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); n];
        distribute_chunks(MappingStrategy::Identity, n, 0, &chunks1, &mut slices);
        distribute_chunks(MappingStrategy::Identity, n, 1, &chunks2, &mut slices);

        // Both stripes should accumulate in each slice
        assert_eq!(slices[0], vec![1, 3]);
        assert_eq!(slices[1], vec![2, 4]);
    }

    #[test]
    fn test_layout_valid() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        // pick_stripe_size selects 100KB for small blobs, so use 250KB to get 3 stripes
        let payload = mk(250_000);
        let chunks = slicer.encode(&payload).unwrap();

        let refs = to_refs(&chunks);
        let meta = SliceMetadata::from_slice(&chunks[0]).unwrap();

        let (num_stripes, chunk_size) = validate_layout(&refs, &meta).unwrap();
        assert_eq!(num_stripes, 3); // 250KB / 100KB = 3 stripes
        assert!(chunk_size > 0);
    }

    #[test]
    fn test_layout_mismatch() {
        let mut slicer = Slicer::with_stripe_size(ClayCoder::new(20, 10, 19), 1024);
        let payload = mk(2000);
        let mut chunks = slicer.encode(&payload).unwrap();

        // Corrupt one chunk by truncating it
        chunks[1].pop();

        let refs = to_refs(&chunks);
        let meta = SliceMetadata::from_slice(&chunks[0]).unwrap();

        let result = validate_layout(&refs, &meta);
        assert!(matches!(result, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn test_chunk_index_differentiates_commitments() {
        use crate::blob_merkle_root;

        // Encode identical zero data at two different chunk indices
        let zeros = vec![0u8; 1000];

        let mut slicer_a = Slicer::new(ClayCoder::from_params(
            tape_core::encoding::ClayParams::default(),
        ));
        slicer_a.set_chunk_index(0);
        let slices_a = slicer_a.encode(&zeros).unwrap();

        let mut slicer_b = Slicer::new(ClayCoder::from_params(
            tape_core::encoding::ClayParams::default(),
        ));
        slicer_b.set_chunk_index(1);
        let slices_b = slicer_b.encode(&zeros).unwrap();

        let root_a = blob_merkle_root(&slices_a);
        let root_b = blob_merkle_root(&slices_b);

        assert_ne!(root_a, root_b, "identical data at different chunk indices must produce different commitments");
    }

}
