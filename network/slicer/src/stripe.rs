//! Shared striping logic for StripedSlicer and RotatedSlicer.
//!
//! Both slicers split blobs into stripes and encode each stripe separately.
//! The difference is how shards map to output slices:
//! - StripedSlicer: identity mapping (shard N -> slice N)
//! - RotatedSlicer: rotated mapping for fair load distribution

use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::slice_index::SliceIndex;
use crate::types::{Blob, Slice};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Optimal stripe size (512 KB) based on benchmarks.
pub const DEFAULT_STRIPE_SIZE: usize = 512 * 1024;

/// Rotation step per stripe (coprime with SLICE_COUNT for full coverage).
pub const ROTATION_STEP: usize = CODING_SLICES;

/// Mapping strategy for shard-to-slice assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingStrategy {
    /// Identity mapping: shard N -> slice N (no rotation)
    Identity,
    /// Rotated mapping: shard N -> slice (N + stripe * ROTATION_STEP) % SLICE_COUNT
    Rotated,
}

/// Forward mapping: (stripe, shard) -> slice
#[inline]
pub fn shard_to_slice(strategy: MappingStrategy, stripe_idx: usize, shard_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => shard_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
            (shard_idx + offset) % SLICE_COUNT
        }
    }
}

/// Inverse mapping: (stripe, slice) -> shard
#[inline]
pub fn slice_to_shard(strategy: MappingStrategy, stripe_idx: usize, slice_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => slice_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
            (slice_idx + SLICE_COUNT - offset) % SLICE_COUNT
        }
    }
}

/// Round up `n` to be divisible by `divisor`.
#[inline]
pub fn round_up_to(n: usize, divisor: usize) -> usize {
    ((n + divisor - 1) / divisor) * divisor
}

/// Core striped encoder/decoder with configurable mapping strategy.
pub struct StripedCodec {
    pub stripe_size: usize,
    pub strategy: MappingStrategy,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl StripedCodec {
    /// Create a new codec with the given stripe size and mapping strategy.
    pub fn new(stripe_size: usize, strategy: MappingStrategy) -> Self {
        assert!(stripe_size > 0, "stripe_size must be > 0");

        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        let encoder = ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS decoder init");

        Self {
            stripe_size,
            strategy,
            encoder,
            decoder,
        }
    }

    /// Encode a blob into SLICE_COUNT slices.
    pub fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();

        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        // Initialize output slices
        let mut slices: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(num_stripes * chunk_size + 8))
            .collect();

        for s in 0..num_stripes {
            let start = s * self.stripe_size;
            let end = (start + self.stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            // Pad stripe for RS encoding
            let mut padded = stripe_data.to_vec();
            padded.resize(padded_stripe, 0);

            self.encoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| EncodeError::TooMuchData)?;

            for chunk in padded.chunks(chunk_size) {
                self.encoder
                    .add_original_shard(chunk)
                    .map_err(|_| EncodeError::TooMuchData)?;
            }

            let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

            // Append data shards with mapping
            for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                slices[slice_idx].extend_from_slice(chunk);
            }

            // Append parity shards with mapping
            for (parity_idx, shard) in result.recovery_iter().enumerate() {
                let shard_idx = DATA_SLICES + parity_idx;
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                slices[slice_idx].extend_from_slice(shard);
            }
        }

        // Append blob length metadata
        let len_bytes = (blob_len as u64).to_le_bytes();
        for slice in &mut slices {
            slice.extend_from_slice(&len_bytes);
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    /// Decode slices back into the original blob.
    pub fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }

        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;

        const METADATA_LEN: usize = 8;
        if sample.data.len() < METADATA_LEN {
            return Err(DecodeError::InvalidLayout);
        }

        let blob_len = u64::from_le_bytes(
            sample.data[sample.data.len() - METADATA_LEN..]
                .try_into()
                .map_err(|_| DecodeError::InvalidLayout)?,
        ) as usize;

        if blob_len == 0 {
            return Ok(Blob::from(Vec::new()));
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        let expected_slice_len = num_stripes * chunk_size + METADATA_LEN;
        for slice in slices.iter().flatten() {
            if slice.data.len() != expected_slice_len {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let mut output = Vec::with_capacity(blob_len);

        for s in 0..num_stripes {
            let chunk_offset = s * chunk_size;

            self.decoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| DecodeError::TooMuchData)?;

            // Feed available shards with inverse mapping
            for (slice_idx, slice_opt) in slices.iter().enumerate() {
                if let Some(slice) = slice_opt {
                    let shard_idx = slice_to_shard(self.strategy, s, slice_idx);
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

            let result = self.decoder.decode().map_err(|_| DecodeError::BadEncoding)?;

            // Reassemble stripe data
            let mut stripe_data = Vec::with_capacity(padded_stripe);
            for data_shard_idx in 0..DATA_SLICES {
                let slice_idx = shard_to_slice(self.strategy, s, data_shard_idx);
                let chunk = match &slices[slice_idx] {
                    Some(slice) => &slice.data[chunk_offset..chunk_offset + chunk_size],
                    None => result
                        .restored_original(data_shard_idx)
                        .ok_or(DecodeError::InvalidLayout)?,
                };
                stripe_data.extend_from_slice(chunk);
            }

            let take = if s == num_stripes - 1 {
                blob_len - output.len()
            } else {
                self.stripe_size
            };
            output.extend_from_slice(&stripe_data[..take]);
        }

        Ok(Blob::from(output))
    }

    fn encode_empty_blob(&mut self) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;
        let padded = vec![0u8; padded_stripe];

        self.encoder
            .reset(DATA_SLICES, CODING_SLICES, chunk_size)
            .map_err(|_| EncodeError::TooMuchData)?;

        for chunk in padded.chunks(chunk_size) {
            self.encoder
                .add_original_shard(chunk)
                .map_err(|_| EncodeError::TooMuchData)?;
        }

        let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); SLICE_COUNT];

        // Data shards with mapping (stripe 0)
        for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx] = chunk.to_vec();
        }

        // Parity shards with mapping (stripe 0)
        for (parity_idx, shard) in result.recovery_iter().enumerate() {
            let shard_idx = DATA_SLICES + parity_idx;
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx] = shard.to_vec();
        }

        // Append metadata
        for slice in &mut slices {
            slice.extend_from_slice(&0u64.to_le_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_mapping() {
        for stripe in 0..10 {
            for shard in 0..SLICE_COUNT {
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
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 1024;
        let mut slice_hits = vec![0usize; SLICE_COUNT];

        for stripe in 0..num_stripes {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        // Each slice should be hit equally
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, num_stripes, "slice {} hit count mismatch", i);
        }
    }
}
