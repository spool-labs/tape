use super::api::Slicer;
use super::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::errors::{DecodeError, EncodeError};
use super::slice_index::SliceIndex;
use super::striped::DEFAULT_STRIPE_SIZE;
use super::types::{Blob, Slice};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Rotation step per stripe (coprime with SLICE_COUNT for full coverage).
/// Using CODING_SLICES (341) since gcd(341, 1024) = 1.
pub const ROTATION_STEP: usize = CODING_SLICES;

/// A rotated slicer that extends striped encoding with per-stripe rotation.
///
/// This provides fair load distribution across all 1024 nodes by rotating
/// the shard-to-slice mapping for each stripe. Over many stripes, each node
/// receives approximately equal amounts of data and parity chunks.
///
/// The rotation uses a step of CODING_SLICES (341), which is coprime with
/// SLICE_COUNT (1024), ensuring full coverage of all slices.
pub struct RotatedSlicer {
    stripe_size: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl RotatedSlicer {
    /// Create a new RotatedSlicer with the default stripe size (512 KB).
    pub fn new() -> Self {
        Self::with_stripe_size(DEFAULT_STRIPE_SIZE)
    }

    /// Create a new RotatedSlicer with a custom stripe size.
    pub fn with_stripe_size(stripe_size: usize) -> Self {
        assert!(stripe_size > 0, "stripe_size must be > 0");

        // Calculate the max chunk size per stripe
        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        let encoder = ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS decoder init");

        Self {
            stripe_size,
            encoder,
            decoder,
        }
    }

    /// Get the stripe size used by this slicer.
    pub fn stripe_size(&self) -> usize {
        self.stripe_size
    }
}

impl Default for RotatedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

/// Round up `n` to be divisible by `divisor`.
#[inline]
fn round_up_to(n: usize, divisor: usize) -> usize {
    ((n + divisor - 1) / divisor) * divisor
}

/// Forward mapping: (stripe, shard) -> slice.
/// Applies rotation based on stripe index for fair distribution.
#[inline]
fn shard_to_slice(stripe_idx: usize, shard_idx: usize) -> usize {
    let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
    (shard_idx + offset) % SLICE_COUNT
}

/// Inverse mapping: (stripe, slice) -> shard.
/// Reverses the rotation to recover original shard index.
#[inline]
fn slice_to_shard(stripe_idx: usize, slice_idx: usize) -> usize {
    let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
    (slice_idx + SLICE_COUNT - offset) % SLICE_COUNT
}

impl Slicer for RotatedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();
        let stripe_size = self.stripe_size;

        // Handle empty blob
        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let num_stripes = (blob_len + stripe_size - 1) / stripe_size;

        // Pre-calculate chunk size per stripe
        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        // Initialize output slices with capacity for all stripes plus metadata
        let mut slices: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(num_stripes * chunk_size + 8))
            .collect();

        for s in 0..num_stripes {
            let start = s * stripe_size;
            let end = (start + stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            // Pad stripe to required size for RS encoding
            let mut padded = stripe_data.to_vec();
            padded.resize(padded_stripe, 0);

            // Reset encoder for this stripe size
            self.encoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| EncodeError::TooMuchData)?;

            // Feed data shards to encoder
            for chunk in padded.chunks(chunk_size) {
                self.encoder
                    .add_original_shard(chunk)
                    .map_err(|_| EncodeError::TooMuchData)?;
            }

            // Encode to get parity shards
            let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

            // Append data shards with rotation
            for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
                let slice_idx = shard_to_slice(s, shard_idx);
                slices[slice_idx].extend_from_slice(chunk);
            }

            // Append parity shards with rotation
            for (parity_idx, shard) in result.recovery_iter().enumerate() {
                let shard_idx = DATA_SLICES + parity_idx;
                let slice_idx = shard_to_slice(s, shard_idx);
                slices[slice_idx].extend_from_slice(shard);
            }
        }

        // Append blob length as metadata suffix (8 bytes) to all slices
        let len_bytes = (blob_len as u64).to_le_bytes();
        for slice in &mut slices {
            slice.extend_from_slice(&len_bytes);
        }

        // Convert to Slice array
        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let idx = SliceIndex::new(i).expect("index in range");
                Slice::new(idx, data)
            })
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        // Count present slices
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Find a present slice to extract metadata
        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;

        // Extract blob length from last 8 bytes
        let metadata_len = 8;
        if sample.data.len() < metadata_len {
            return Err(DecodeError::InvalidLayout);
        }

        let blob_len = u64::from_le_bytes(
            sample.data[sample.data.len() - metadata_len..]
                .try_into()
                .map_err(|_| DecodeError::InvalidLayout)?,
        ) as usize;

        // Handle empty blob
        if blob_len == 0 {
            return Ok(Blob::from(Vec::new()));
        }

        // Calculate stripe parameters
        let stripe_size = self.stripe_size;
        let num_stripes = (blob_len + stripe_size - 1) / stripe_size;
        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        // Verify all present slices have consistent size
        let expected_slice_len = num_stripes * chunk_size + metadata_len;
        for slice in slices.iter().flatten() {
            if slice.data.len() != expected_slice_len {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let mut output = Vec::with_capacity(blob_len);

        for s in 0..num_stripes {
            let chunk_offset = s * chunk_size;

            // Reset decoder for this stripe
            self.decoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| DecodeError::TooMuchData)?;

            // Feed available shards to decoder with rotation reversal
            for (slice_idx, slice_opt) in slices.iter().enumerate() {
                if let Some(slice) = slice_opt {
                    let shard_idx = slice_to_shard(s, slice_idx);
                    let chunk = &slice.data[chunk_offset..chunk_offset + chunk_size];

                    if shard_idx < DATA_SLICES {
                        self.decoder
                            .add_original_shard(shard_idx, chunk)
                            .map_err(|_| DecodeError::InvalidLayout)?;
                    } else {
                        self.decoder
                            .add_recovery_shard(shard_idx - DATA_SLICES, chunk)
                            .map_err(|_| DecodeError::InvalidLayout)?;
                    }
                }
            }

            // Decode to recover missing data shards
            let result = self.decoder.decode().map_err(|_| DecodeError::BadEncoding)?;

            // Reassemble the stripe data from data shards in order
            let mut stripe_data = Vec::with_capacity(padded_stripe);
            for data_shard_idx in 0..DATA_SLICES {
                // Find which slice contains this shard for this stripe
                let slice_idx = shard_to_slice(s, data_shard_idx);

                let chunk = match &slices[slice_idx] {
                    Some(slice) => &slice.data[chunk_offset..chunk_offset + chunk_size],
                    None => result
                        .restored_original(data_shard_idx)
                        .ok_or(DecodeError::InvalidLayout)?,
                };
                stripe_data.extend_from_slice(chunk);
            }

            // Append to output (trim to actual size for last stripe)
            let take = if s == num_stripes - 1 {
                blob_len - output.len()
            } else {
                stripe_size
            };
            output.extend_from_slice(&stripe_data[..take]);
        }

        Ok(Blob::from(output))
    }
}

impl RotatedSlicer {
    /// Encode an empty blob (special case).
    fn encode_empty_blob(&mut self) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        // Create a single stripe of zeros
        let padded = vec![0u8; padded_stripe];

        // Reset encoder
        self.encoder
            .reset(DATA_SLICES, CODING_SLICES, chunk_size)
            .map_err(|_| EncodeError::TooMuchData)?;

        // Feed data shards
        for chunk in padded.chunks(chunk_size) {
            self.encoder
                .add_original_shard(chunk)
                .map_err(|_| EncodeError::TooMuchData)?;
        }

        // Encode
        let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

        // Build slices with just one stripe plus metadata (stripe 0, no rotation effect)
        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); SLICE_COUNT];

        // Data shards with rotation for stripe 0
        for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
            let slice_idx = shard_to_slice(0, shard_idx);
            slices[slice_idx] = chunk.to_vec();
        }

        // Parity shards with rotation for stripe 0
        for (parity_idx, shard) in result.recovery_iter().enumerate() {
            let shard_idx = DATA_SLICES + parity_idx;
            let slice_idx = shard_to_slice(0, shard_idx);
            slices[slice_idx] = shard.to_vec();
        }

        // Append metadata (blob_len = 0) to all slices
        for slice in &mut slices {
            slice.extend_from_slice(&0u64.to_le_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let idx = SliceIndex::new(i).expect("index in range");
                Slice::new(idx, data)
            })
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
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
    fn test_rotation_step() {
        assert_eq!(ROTATION_STEP, CODING_SLICES);
        // Verify coprime with SLICE_COUNT
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, SLICE_COUNT), 1);
    }

    #[test]
    fn test_rotation_inverse() {
        // Verify that slice_to_shard is the inverse of shard_to_slice
        for stripe in 0..10 {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(stripe, shard);
                let recovered_shard = slice_to_shard(stripe, slice);
                assert_eq!(shard, recovered_shard, "stripe={}, shard={}", stripe, shard);
            }
        }
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024); // 1 KB stripes for testing
        let payload = mk(500);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024); // 1 KB stripes
        let payload = mk(5000); // ~5 stripes
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
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep exactly DATA_SLICES slices (first 683)
        let keep_indices: Vec<usize> = (0..DATA_SLICES).collect();
        keep_only(&mut opt, &keep_indices);

        // Make sure we have enough
        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= DATA_SLICES);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
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
        // Verify that rotation distributes shards across all slices over multiple stripes
        let num_stripes = 1024;
        let mut slice_hits = vec![0usize; SLICE_COUNT];

        for stripe in 0..num_stripes {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        // With ROTATION_STEP coprime to SLICE_COUNT, each slice should be hit equally
        // Total hits = num_stripes * SLICE_COUNT
        let expected_hits_per_slice = num_stripes;
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, expected_hits_per_slice, "slice {} has {} hits, expected {}", i, hits, expected_hits_per_slice);
        }
    }
}
