//! Striped erasure coder with optional rotation.
//!
//! `Slicer<C>` wraps any `ErasureCoder` implementation and adds:
//! - Stripe splitting (size derived per blob)
//! - Metadata suffix (blob_len, stripe_size for decoding)
//! - Optional rotation mapping for fair load distribution

use tape_core::encoding::{ErasureCode, ProfileError};
use tape_core::types::ChunkNumber;

use crate::stripe::{derive_stripe_size, stripe_size_accepted, STRIPE_CAP};
use crate::errors::{DecodeError, EncodeError};
use crate::metadata::SliceMetadata;
use crate::range::StripeWindow;
use crate::striped::StripedCoder;
use crate::ErasureCoder;

const fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let rest = a % b;
        a = b;
        b = rest;
    }
    a
}

/// Rotation step per stripe for a group of n slices.
///
/// The smallest value at or above n/3 that shares no factor with n, so n
/// stripes visit every position and consecutive stripes land far apart. A
/// fixed step fails at widths it divides: 7 would pin every stripe to one
/// position at n = 7. At n = 20 this gives 7. At n <= 2 the only such step is
/// 1, where rotation does nothing.
#[inline]
pub const fn rotation_step(n: usize) -> usize {
    if n <= 2 {
        return 1;
    }

    let mut step = n / 3;
    if step < 2 {
        step = 2;
    }
    while gcd(step, n) != 1 {
        step += 1;
    }
    step
}

/// Mapping strategy for shard-to-slice assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingStrategy {
    /// Identity mapping: shard N -> slice N (no rotation)
    Identity,
    /// Rotated mapping: shard N -> slice (N + stripe * rotation_step(n)) % n
    Rotated,
}

/// Forward mapping: (stripe, shard) -> slice
#[inline]
pub fn shard_to_slice(strategy: MappingStrategy, n: usize, stripe_idx: usize, shard_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => shard_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * rotation_step(n)) % n;
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
            let offset = (stripe_idx * rotation_step(n)) % n;
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
    let num_stripes = blob_len.div_ceil(stripe_size);

    // Determine chunk_size from first sample: (total_len - metadata) / num_stripes
    let sample_len = chunks[0].1.len();
    let total_data_len = sample_len.saturating_sub(SliceMetadata::SIZE);

    if total_data_len == 0 || !total_data_len.is_multiple_of(num_stripes) {
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
/// // Production: the rotated slicer for the network's profile
/// let mut slicer = Slicer::try_from_profile(profile)?;
/// let chunks = slicer.encode(&data)?;
///
/// // Striped only (no rotation)
/// let mut slicer = Slicer::new(ClayCoder::from_params(params));
/// ```
pub struct Slicer<C: ErasureCoder> {
    pub coder: C,
    /// Stripe size of the last encode; 0 until the first one.
    pub stripe_size: usize,
    pub strategy: MappingStrategy,
    /// Chunk/group index embedded in slice metadata. Ensures that identical
    /// data chunks at different positions produce distinct commitments.
    pub chunk_index: ChunkNumber,
    /// Largest stripe this slicer derives, on encode and when checking what it
    /// reads. Every production path runs at the constant. Only tests and the
    /// measurement harness vary it, to reach many-stripe geometry without
    /// multi-megabyte payloads and to sweep stripe size against throughput.
    pub stripe_cap: usize,
}

impl<C: ErasureCoder> Slicer<C> {
    fn build(coder: C, strategy: MappingStrategy, cap: usize) -> Self {
        Self {
            coder,
            stripe_size: 0,
            strategy,
            chunk_index: ChunkNumber(0),
            stripe_cap: cap,
        }
    }

    /// Create a new striped coder with identity mapping (no rotation).
    ///
    /// Uses the production stripe cap.
    pub fn new(coder: C) -> Self {
        Self::build(coder, MappingStrategy::Identity, STRIPE_CAP)
    }

    /// Create a new striped coder with rotation (production mode).
    ///
    /// Rotation ensures fair load distribution across all nodes.
    pub fn with_rotation(coder: C) -> Self {
        Self::build(coder, MappingStrategy::Rotated, STRIPE_CAP)
    }

    /// Create with a stripe cap other than the production one. Sizing still
    /// derives from the blob; only the ceiling moves. Test affordance, not a
    /// production path.
    pub fn with_stripe_cap(coder: C, cap: usize) -> Self {
        Self::build(coder, MappingStrategy::Identity, cap)
    }

    /// Set the stripe cap. Test affordance, not a production path.
    pub fn set_stripe_cap(&mut self, cap: usize) {
        self.stripe_cap = cap;
    }

    /// Stripe size this slicer derives for a blob of the given length.
    pub fn derived_stripe_size(&self, blob_len: usize) -> usize {
        derive_stripe_size(blob_len, self.coder.stripe_alignment(), self.stripe_cap)
    }

    /// Whether a stripe size read off a slice is the one this blob length
    /// derives. Anything else is not something this scheme could have written.
    pub fn accepts_stripe_size(&self, stripe_size: usize, blob_len: usize) -> bool {
        stripe_size_accepted(
            stripe_size,
            blob_len,
            self.coder.stripe_alignment(),
            self.stripe_cap,
        )
    }

    /// Set the chunk index for metadata. Ensures identical data at different
    /// positions produces distinct slice commitments.
    pub fn set_chunk_index(&mut self, index: ChunkNumber) {
        self.chunk_index = index;
    }

    /// Get the current stripe size.
    pub fn stripe_size(&self) -> usize {
        self.stripe_size
    }

    /// Get the mapping strategy.
    pub fn strategy(&self) -> MappingStrategy {
        self.strategy
    }

    /// Read the metadata off a slice, refusing a stripe size this slicer
    /// would not have written for the carried blob length.
    pub fn read_metadata(&self, slice_data: &[u8]) -> Result<SliceMetadata, DecodeError> {
        let metadata = SliceMetadata::parse(slice_data)?;
        if !self.accepts_stripe_size(metadata.stripe_size(), metadata.blob_len()) {
            return Err(DecodeError::InvalidLayout);
        }

        Ok(metadata)
    }
}

impl Slicer<StripedCoder> {
    /// Create the rotated slicer a network's profile selects.
    pub fn try_from_profile(profile: ErasureCode) -> Result<Self, ProfileError> {
        Ok(Self::with_rotation(StripedCoder::try_new(profile)?))
    }
}

impl<C: ErasureCoder + Sync> ErasureCoder for Slicer<C> {
    fn k(&self) -> usize {
        self.coder.k()
    }

    fn m(&self) -> usize {
        self.coder.m()
    }

    fn stripe_alignment(&self) -> usize {
        self.coder.stripe_alignment()
    }

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        let blob_len = data.len();

        // Derive the stripe size from the blob
        self.stripe_size = self.derived_stripe_size(blob_len);

        // Handle empty blob
        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let n = self.n();
        let num_stripes = blob_len.div_ceil(self.stripe_size);

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

        // Encode remaining stripes. Only the last one can fall short, and the
        // derived stripe size is a multiple of the coder's alignment, so
        // padding it up front is what keeps chunk sizes uniform.
        let mut padded = Vec::new();
        for s in 1..num_stripes {
            let start = s * self.stripe_size;
            let end = (start + self.stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            let chunks = if stripe_data.len() == self.stripe_size {
                self.coder.encode(stripe_data)?
            } else {
                padded.clear();
                padded.extend_from_slice(stripe_data);
                padded.resize(self.stripe_size, 0);
                self.coder.encode(&padded)?
            };

            distribute_chunks(self.strategy, n, s, &chunks, &mut slices);
        }

        // Append metadata (includes chunk_index for position-dependent commitment)
        let mut metadata = SliceMetadata::new(blob_len, self.stripe_size);
        metadata.chunk_index = self.chunk_index;
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        Ok(slices)
    }

    fn decode(&self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.is_empty() {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Parse metadata from any available chunk
        let metadata = self.read_metadata(chunks[0].1)?;

        if chunks.len() < self.coder.k() {
            return Err(DecodeError::NotEnoughSlices);
        }

        let blob_len = metadata.blob_len();
        if blob_len == 0 {
            return Ok(Vec::new());
        }

        let (num_stripes, chunk_size) = validate_layout(chunks, &metadata)?;
        let mut by_slice: Vec<Option<&[u8]>> = vec![None; self.n()];
        for &(slice_idx, data) in chunks {
            if let Some(slot) = by_slice.get_mut(slice_idx) {
                *slot = Some(data);
            }
        }

        let layout = StripeLayout {
            stripe_size: metadata.stripe_size(),
            chunk_size,
        };
        let mut output = vec![0u8; blob_len];
        let threads = decode_threads(num_stripes);
        if threads == 1 {
            self.decode_stripes(&by_slice, layout, 0, 0, &mut output)?;
            return Ok(output);
        }

        let stripes_per_thread = num_stripes.div_ceil(threads);
        let window = stripes_per_thread * layout.stripe_size;
        let by_slice = &by_slice;
        let results: Vec<Result<(), DecodeError>> = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(threads);
            for (part, out) in output.chunks_mut(window).enumerate() {
                let first = part * stripes_per_thread;
                handles.push(scope.spawn(move || self.decode_stripes(by_slice, layout, first, 0, out)));
            }
            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                results.push(handle.join().unwrap_or(Err(DecodeError::BadEncoding)));
            }
            results
        });
        for result in results {
            result?;
        }

        Ok(output)
    }
}

/// A decode spreads its stripes over at most this many threads
pub const DECODE_THREADS: usize = 8;

/// A thread earns its spawn only with at least this many stripes to decode
const MIN_STRIPES_PER_THREAD: usize = 2;

/// Where each stripe sits in the blob and in every slice
#[derive(Clone, Copy, Debug)]
struct StripeLayout {
    stripe_size: usize,
    chunk_size: usize,
}

fn decode_threads(num_stripes: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
    (num_stripes / MIN_STRIPES_PER_THREAD).min(cores).min(DECODE_THREADS).max(1)
}

impl<C: ErasureCoder + Sync> Slicer<C> {
    /// Decode the stripes a range covers from slice windows that start at its slice offset
    pub fn decode_window(
        &self,
        windows: &[(usize, &[u8])],
        blob_len: usize,
        stripe_size: usize,
        chunk_size: usize,
        window: StripeWindow,
    ) -> Result<Vec<u8>, DecodeError> {
        if windows.len() < self.coder.k() {
            return Err(DecodeError::NotEnoughSlices);
        }
        let first_byte = window.first_stripe * stripe_size;
        let needed = (window.first_stripe + window.stripe_count) * chunk_size;
        if first_byte >= blob_len || needed < window.slice_offset {
            return Err(DecodeError::InvalidLayout);
        }

        let mut by_slice: Vec<Option<&[u8]>> = vec![None; self.n()];
        for &(slice_idx, data) in windows {
            if data.len() < needed - window.slice_offset {
                return Err(DecodeError::InvalidLayout);
            }
            if let Some(slot) = by_slice.get_mut(slice_idx) {
                *slot = Some(data);
            }
        }

        let layout = StripeLayout {
            stripe_size,
            chunk_size,
        };
        let mut output = vec![0u8; (window.stripe_count * stripe_size).min(blob_len - first_byte)];
        self.decode_stripes(&by_slice, layout, window.first_stripe, window.slice_offset, &mut output)?;
        Ok(output)
    }

    /// Decode consecutive stripes from `first` into `out`, reading slices that begin at `base` bytes in
    fn decode_stripes(
        &self,
        by_slice: &[Option<&[u8]>],
        layout: StripeLayout,
        first: usize,
        base: usize,
        out: &mut [u8],
    ) -> Result<(), DecodeError> {
        let n = self.n();
        let mut stripe_chunks: Vec<(usize, &[u8])> = Vec::with_capacity(n);
        for (offset, window) in out.chunks_mut(layout.stripe_size).enumerate() {
            let stripe = first + offset;
            let chunk_offset = stripe * layout.chunk_size - base;

            stripe_chunks.clear();
            for shard_idx in 0..n {
                let slice_idx = shard_to_slice(self.strategy, n, stripe, shard_idx);
                if let Some(slice_data) = by_slice[slice_idx] {
                    stripe_chunks.push((shard_idx, &slice_data[chunk_offset..chunk_offset + layout.chunk_size]));
                }
            }

            let stripe_data = self.coder.decode(&stripe_chunks)?;
            let Some(decoded) = stripe_data.get(..window.len()) else {
                return Err(DecodeError::InvalidLayout);
            };
            window.copy_from_slice(decoded);
        }
        Ok(())
    }
}

impl<C: ErasureCoder> Slicer<C> {
    fn encode_empty_blob(&mut self) -> Result<Vec<Vec<u8>>, EncodeError> {
        let n = self.coder.n();

        // Encode a full stripe of zeros
        let empty = vec![0u8; self.stripe_size];
        let chunks = self.coder.encode(&empty)?;
        let chunk_size = chunks[0].len();

        let mut slices: Vec<Vec<u8>> = vec![Vec::with_capacity(chunk_size + SliceMetadata::SIZE); n];
        distribute_chunks(self.strategy, n, 0, &chunks, &mut slices);

        // Append metadata (blob_len = 0 for empty blob)
        let mut metadata = SliceMetadata::new(0, self.stripe_size);
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
    use crate::{num_stripes, stripe_window, ClayCoder};
    use tape_core::encoding::{ClayParams, RSParams, RepParams, TAPENET_CLAY};

    const N: usize = 20; // k=7 + m=13 (tapenet's Clay)

    /// Stripe cap for the small-payload tests. The test coder aligns to
    /// k * alpha * 2 = 1400 bytes, so this is one stripe per alignment unit
    /// and a few kilobytes still span several stripes.
    const TEST_CAP: usize = 1_400;

    fn test_coder() -> ClayCoder {
        ClayCoder::from_params(TAPENET_CLAY)
    }

    /// The rotated slicer tapenet runs.
    fn tapenet() -> Slicer<ClayCoder> {
        Slicer::with_rotation(test_coder())
    }

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

    // tapenet's step stays 7, so its slice layout doesn't move
    #[test]
    fn tapenet_step() {
        assert_eq!(rotation_step(N), 7);
    }

    // at every width the step shares no factor with n, so n stripes put one
    // shard on every slice position
    #[test]
    fn step_covers() {
        for n in 1..=N {
            assert_eq!(gcd(rotation_step(n), n), 1, "n {n}");

            let mut visited = vec![false; n];
            for stripe in 0..n {
                visited[shard_to_slice(MappingStrategy::Rotated, n, stripe, 0)] = true;
            }
            assert!(visited.iter().all(|&is_visited| is_visited), "n {n}");
        }
    }

    #[test]
    fn test_distribution() {
        let num_stripes = 100;
        let mut slice_hits = [0usize; N];

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
    fn test_stripe_size_derives_from_blob() {
        let mut slicer = tapenet();

        // one stripe under the cap, several equal stripes above it
        for (len, stripes) in [(1usize, 1usize), (500_000, 1), (1_000_000, 1), (2_500_000, 3)] {
            slicer.encode(&mk(len)).unwrap();
            assert_eq!(num_stripes(len, slicer.stripe_size()), stripes, "blob_len {len}");
        }
    }

    #[test]
    fn test_small_identity() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = mk(500);
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    // a range decodes from window cuts of k slices and matches the blob bytes
    #[test]
    fn window_decode() {
        let blob = mk(4 * 1024 * 1024 + 333);
        let mut slicer = tapenet();
        let slices = slicer.encode(&blob).expect("encode");
        let stripe_size = slicer.stripe_size();
        let stripes = num_stripes(blob.len(), stripe_size);
        let chunk_size = (slices[0].len() - SliceMetadata::SIZE) / stripes;
        let keep = [1usize, 4, 6, 9, 13, 17, 19];

        for (start, end) in [(stripe_size + 7, stripe_size + 4103), (stripe_size - 3, stripe_size * 2 + 9), (blob.len() - 50, blob.len())] {
            let window = stripe_window(blob.len(), stripe_size, chunk_size, start, end).expect("window");
            let cuts: Vec<(usize, &[u8])> = keep
                .iter()
                .map(|&idx| (idx, &slices[idx][window.slice_offset..window.slice_offset + window.slice_len]))
                .collect();

            let decoded = slicer.decode_window(&cuts, blob.len(), stripe_size, chunk_size, window).expect("decode");

            assert_eq!(&decoded[window.skip..window.skip + window.take], &blob[start..end]);
        }
    }

    #[test]
    fn test_small_rotated() {
        let mut slicer = tapenet();
        slicer.set_stripe_cap(TEST_CAP);
        let payload = mk(500);
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_multi_stripe() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_empty() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = Vec::new();
        let chunks = slicer.encode(&payload).unwrap();
        assert_eq!(chunks.len(), N);

        let refs = to_refs(&chunks);
        let restored = slicer.decode(&refs).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_data_only() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let k = slicer.k();
        let payload = mk(3000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..k).collect::<Vec<_>>());
        let restored = slicer.decode(&partial).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn test_missing_slices() {
        let mut slicer = tapenet();
        slicer.set_stripe_cap(TEST_CAP);
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
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let k = slicer.k();
        let payload = mk(1000);
        let chunks = slicer.encode(&payload).unwrap();

        let partial = keep_only(&chunks, &(0..(k - 1)).collect::<Vec<_>>());
        let res = slicer.decode(&partial);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_uniform_slices() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = mk(5000);
        let chunks = slicer.encode(&payload).unwrap();
        let first_len = chunks[0].len();
        for chunk in &chunks {
            assert_eq!(chunk.len(), first_len);
        }
    }

    #[test]
    fn test_tapenet() {
        let mut slicer = tapenet();
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
        let slicer = Slicer::new(test_coder());
        assert_eq!(slicer.k(), 7);
        assert_eq!(slicer.m(), 13);
        assert_eq!(slicer.n(), 20);
    }

    #[test]
    fn test_metadata() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = mk(2000);
        let chunks = slicer.encode(&payload).unwrap();

        // the derived size is carried in the metadata
        let meta = SliceMetadata::parse(&chunks[0]).unwrap();
        assert_eq!(meta.blob_len(), 2000);
        assert_eq!(meta.stripe_size(), slicer.derived_stripe_size(2000));
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

        // Stripe 0: offset = 0, so same as identity
        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); n];
        distribute_chunks(MappingStrategy::Rotated, n, 0, &chunks, &mut slices);
        assert_eq!(slices[0], vec![1, 2]);
        assert_eq!(slices[1], vec![3, 4]);

        // Stripe 1: offset = rotation_step(4) = 3
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
        let mut slicer = tapenet();
        // derived sizing splits 2.5MB into three equal stripes under the cap
        let payload = mk(2_500_000);
        let chunks = slicer.encode(&payload).unwrap();

        let refs = to_refs(&chunks);
        let meta = slicer.read_metadata(&chunks[0]).unwrap();

        let (num_stripes, chunk_size) = validate_layout(&refs, &meta).unwrap();
        assert_eq!(num_stripes, 3);
        assert!(chunk_size > 0);
    }

    #[test]
    fn test_layout_mismatch() {
        let mut slicer = Slicer::with_stripe_cap(test_coder(), TEST_CAP);
        let payload = mk(2000);
        let mut chunks = slicer.encode(&payload).unwrap();

        // Corrupt one chunk by truncating it
        chunks[1].pop();

        let refs = to_refs(&chunks);
        let meta = SliceMetadata::parse(&chunks[0]).unwrap();

        let result = validate_layout(&refs, &meta);
        assert!(matches!(result, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn test_chunk_index_differentiates_commitments() {
        use crate::blob_merkle_root;

        // Encode identical zero data at two different chunk indices
        let zeros = vec![0u8; 1000];

        let mut slicer_a = Slicer::new(test_coder());
        slicer_a.set_chunk_index(ChunkNumber(0));
        let slices_a = slicer_a.encode(&zeros).unwrap();

        let mut slicer_b = Slicer::new(test_coder());
        slicer_b.set_chunk_index(ChunkNumber(1));
        let slices_b = slicer_b.encode(&zeros).unwrap();

        let root_a = blob_merkle_root(&slices_a);
        let root_b = blob_merkle_root(&slices_b);

        assert_ne!(root_a, root_b, "identical data at different chunk indices must produce different commitments");
    }

    // a stripe size the blob length doesn't derive is refused, even one that
    // is right for another length
    #[test]
    fn forged_stripe() {
        let slicer = tapenet();
        let honest = SliceMetadata::new(3_000_000, slicer.derived_stripe_size(3_000_000));
        let mut off_by_one = honest;
        off_by_one.stripe_size -= 1;
        let mut other_length = honest;
        other_length.stripe_size = slicer.derived_stripe_size(100_000) as u64;

        assert!(slicer.read_metadata(&honest.to_bytes()).is_ok());
        assert!(slicer.read_metadata(&off_by_one.to_bytes()).is_err());
        assert!(slicer.read_metadata(&other_length.to_bytes()).is_err());
    }

    // every code round-trips through the striped path from any k of its slices
    #[test]
    fn every_code() {
        let profiles = [
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::Clay(ClayParams::new(10, 7, 9)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::Replication(RepParams::new(1)),
            ErasureCode::Replication(RepParams::new(2)),
        ];

        for profile in profiles {
            let n = profile.n() as usize;
            let k = profile.k() as usize;

            for len in [0usize, 1, 500, 5_000] {
                let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
                slicer.set_stripe_cap(1_000);
                let payload = mk(len);

                let slices = slicer.encode(&payload).expect("encode");
                assert_eq!(slices.len(), n, "{profile:?}");

                let last_k = keep_only(&slices, &(n - k..n).collect::<Vec<_>>());
                assert_eq!(slicer.decode(&last_k).expect("decode"), payload, "{profile:?} len {len}");
            }
        }
    }

    // every code decodes a byte range from window cuts of its last k slices,
    // with the chunk length taken from the profile alone
    #[test]
    fn every_code_window() {
        let profiles = [
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::Replication(RepParams::new(2)),
        ];
        let blob = mk(3 * 1024 * 1024 + 333);

        for profile in profiles {
            let n = profile.n() as usize;
            let k = profile.k() as usize;
            let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
            let slices = slicer.encode(&blob).expect("encode");
            let stripe_size = slicer.stripe_size();
            let chunk_size = crate::stripe_chunk_len(profile, stripe_size, blob.len());

            let ranges = [
                (stripe_size + 7, stripe_size + 4103),
                (stripe_size - 3, stripe_size * 2 + 9),
                (blob.len() - 50, blob.len()),
            ];
            for (start, end) in ranges {
                let window = stripe_window(blob.len(), stripe_size, chunk_size, start, end)
                    .expect("window");
                let cuts: Vec<(usize, &[u8])> = (n - k..n)
                    .map(|idx| {
                        let cut = window.slice_offset..window.slice_offset + window.slice_len;
                        (idx, &slices[idx][cut])
                    })
                    .collect();

                let decoded = slicer
                    .decode_window(&cuts, blob.len(), stripe_size, chunk_size, window)
                    .expect("decode");

                assert_eq!(
                    &decoded[window.skip..window.skip + window.take],
                    &blob[start..end],
                    "{profile:?}"
                );
            }
        }
    }

    // one slice short of k decodes nothing, whatever the code
    #[test]
    fn every_code_short() {
        let profiles = [
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
        ];

        for profile in profiles {
            let k = profile.k() as usize;
            let mut slicer = Slicer::try_from_profile(profile).expect("valid profile");
            let slices = slicer.encode(&mk(5_000)).expect("encode");

            let short = keep_only(&slices, &(0..k - 1).collect::<Vec<_>>());

            assert!(matches!(slicer.decode(&short), Err(DecodeError::NotEnoughSlices)), "{profile:?}");
        }
    }
}
