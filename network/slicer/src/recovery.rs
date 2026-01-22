//! RecoveryCodec: inner erasure coding used by NestedSlicer.
//!
//! Encodes a single primary slice (bytes) into SLICE_COUNT "recovery shards":
//! - DATA_SLICES data shards (systematic): chunks of the padded primary
//! - CODING_SLICES parity shards: RS parity from reed_solomon_simd
//!
//! Padding scheme: zero-pad primary to a multiple of DATA_SLICES.
//! No per-shard metadata is appended; sizes are derived from the outer primary size.

use crate::codec::round_up_to;
use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use thiserror::Error;

/// Errors from recovery encoding/decoding.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RecoveryError {
    #[error("not enough shards to reconstruct (need at least {DATA_SLICES})")]
    NotEnoughSlices,

    #[error("invalid shard size: expected {expected}, got {actual}")]
    SizeMismatch { expected: usize, actual: usize },

    #[error("shard index out of range: {index} (max {max})")]
    IndexOutOfRange { index: usize, max: usize },

    #[error("reed-solomon encode failed")]
    EncodeFailed,

    #[error("reed-solomon decode failed")]
    DecodeFailed,
}

/// Error type for encode_row_into that can hold either a callback error or encoding error.
#[derive(Debug, Error)]
pub enum RowEncodeError<E> {
    #[error("callback error: {0}")]
    Callback(E),

    #[error("recovery encode error: {0}")]
    Recovery(#[from] RecoveryError),
}

/// Inner codec configuration derived from a primary slice size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryLayout {
    /// Original primary length (before padding).
    pub primary_len: usize,
    /// Padded length (multiple of DATA_SLICES * 2 for even shard size).
    pub padded_len: usize,
    /// Size of each shard (always even and non-zero).
    pub shard_len: usize,
}

impl RecoveryLayout {
    pub fn new(primary_len: usize) -> Self {
        // Reed-Solomon requires shard_len to be non-zero and even.
        // Round up to multiple of (DATA_SLICES * 2) to ensure even shard_len.
        let chunk = DATA_SLICES * 2;
        let padded_len = round_up_to(primary_len.max(chunk), chunk);
        let shard_len = padded_len / DATA_SLICES;
        debug_assert!(shard_len >= 2 && shard_len % 2 == 0);
        Self {
            primary_len,
            padded_len,
            shard_len,
        }
    }
}

/// Stream-first recovery encoder/decoder.
///
/// Note: reed_solomon_simd allocates internal buffers based on the "max shard size"
/// passed to `new()`. We (re)create the encoder/decoder when the shard size changes.
/// Encoder/decoder are lazily initialized on first use.
pub struct RecoveryCodec {
    encoder: Option<ReedSolomonEncoder>,
    decoder: Option<ReedSolomonDecoder>,
    layout: RecoveryLayout,
}

impl RecoveryCodec {
    /// Create a new RecoveryCodec. Encoder/decoder are lazily initialized.
    pub fn new(primary_size: usize) -> Self {
        let layout = RecoveryLayout::new(primary_size);
        Self {
            encoder: None,
            decoder: None,
            layout,
        }
    }

    /// Current layout.
    pub fn layout(&self) -> RecoveryLayout {
        self.layout
    }

    /// Shard size (bytes).
    pub fn shard_len(&self) -> usize {
        self.layout.shard_len
    }

    /// Reconfigure for a different primary size.
    pub fn reconfigure(&mut self, primary_size: usize) {
        let layout = RecoveryLayout::new(primary_size);
        if layout.shard_len == self.layout.shard_len {
            self.layout = layout;
            return;
        }

        self.layout = layout;
        // Invalidate cached encoder/decoder when shard size changes
        self.encoder = None;
        self.decoder = None;
    }

    /// Get or create the encoder for the current layout.
    fn get_encoder(&mut self) -> Result<&mut ReedSolomonEncoder, RecoveryError> {
        if self.encoder.is_none() {
            self.encoder = Some(
                ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, self.layout.shard_len)
                    .map_err(|_| RecoveryError::EncodeFailed)?,
            );
        }
        Ok(self.encoder.as_mut().unwrap())
    }

    /// Get or create the decoder for the current layout.
    fn get_decoder(&mut self) -> Result<&mut ReedSolomonDecoder, RecoveryError> {
        if self.decoder.is_none() {
            self.decoder = Some(
                ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, self.layout.shard_len)
                    .map_err(|_| RecoveryError::DecodeFailed)?,
            );
        }
        Ok(self.decoder.as_mut().unwrap())
    }

    /// Encode a primary slice and stream *all* SLICE_COUNT shards via `on_shard`.
    ///
    /// `on_shard(j, bytes)` is called in order for:
    /// - j in [0..DATA_SLICES): systematic data shards (views into padded primary)
    /// - j in [DATA_SLICES..SLICE_COUNT): parity shards (views from RS output)
    ///
    /// This avoids materializing Vec<Vec<u8>> for shards.
    ///
    /// Returns the first error from `on_shard`, or RecoveryError if encoding fails.
    pub fn encode_row_into<E>(
        &mut self,
        primary: &[u8],
        mut on_shard: impl FnMut(usize, &[u8]) -> Result<(), E>,
    ) -> Result<(), RowEncodeError<E>> {
        self.reconfigure(primary.len());
        let layout = self.layout;

        // Prepare padded primary
        let mut padded = Vec::with_capacity(layout.padded_len);
        padded.extend_from_slice(primary);
        padded.resize(layout.padded_len, 0);

        let encoder = self.get_encoder()?;
        encoder
            .reset(DATA_SLICES, CODING_SLICES, layout.shard_len)
            .map_err(|_| RecoveryError::EncodeFailed)?;

        // Add originals
        for (i, chunk) in padded.chunks(layout.shard_len).enumerate() {
            debug_assert!(i < DATA_SLICES);
            encoder
                .add_original_shard(chunk)
                .map_err(|_| RecoveryError::EncodeFailed)?;
        }

        // Compute parity
        let result = encoder
            .encode()
            .map_err(|_| RecoveryError::EncodeFailed)?;

        // Stream data shards
        for (j, chunk) in padded.chunks(layout.shard_len).enumerate() {
            on_shard(j, chunk).map_err(RowEncodeError::Callback)?;
        }

        // Stream parity shards
        for (p, shard) in result.recovery_iter().enumerate() {
            let j = DATA_SLICES + p;
            on_shard(j, shard).map_err(RowEncodeError::Callback)?;
        }

        Ok(())
    }

    /// Encode exactly one shard at index `target` (0..SLICE_COUNT-1).
    ///
    /// - If target < DATA_SLICES: returns the corresponding data chunk (copy).
    /// - Else: performs RS encode and returns the parity shard at that parity index (copy).
    pub fn encode_shard(&mut self, primary: &[u8], target: usize) -> Result<Vec<u8>, RecoveryError> {
        if target >= SLICE_COUNT {
            return Err(RecoveryError::IndexOutOfRange {
                index: target,
                max: SLICE_COUNT - 1,
            });
        }

        self.reconfigure(primary.len());
        let layout = self.layout;

        let mut padded = Vec::with_capacity(layout.padded_len);
        padded.extend_from_slice(primary);
        padded.resize(layout.padded_len, 0);

        // Fast path: systematic shard is just a slice of padded data
        if target < DATA_SLICES {
            let start = target * layout.shard_len;
            let end = start + layout.shard_len;
            return Ok(padded[start..end].to_vec());
        }

        // Parity path: must run RS encode
        let encoder = self.get_encoder()?;
        encoder
            .reset(DATA_SLICES, CODING_SLICES, layout.shard_len)
            .map_err(|_| RecoveryError::EncodeFailed)?;

        for chunk in padded.chunks(layout.shard_len) {
            encoder
                .add_original_shard(chunk)
                .map_err(|_| RecoveryError::EncodeFailed)?;
        }

        let result = encoder.encode().map_err(|_| RecoveryError::EncodeFailed)?;
        let parity_index = target - DATA_SLICES;

        // recovery_iter is in parity-index order
        let shard = result
            .recovery_iter()
            .nth(parity_index)
            .ok_or(RecoveryError::EncodeFailed)?;

        Ok(shard.to_vec())
    }

    /// Decode shards back into the original primary bytes.
    ///
    /// `recovery[j]` is the shard at index j (or None if missing).
    /// Requires at least DATA_SLICES total shards present.
    pub fn decode(
        &mut self,
        recovery: &[Option<&[u8]>; SLICE_COUNT],
        original_size: usize,
    ) -> Result<Vec<u8>, RecoveryError> {
        self.reconfigure(original_size);
        let layout = self.layout;

        let present = recovery.iter().filter(|s| s.is_some()).count();
        if present < DATA_SLICES {
            return Err(RecoveryError::NotEnoughSlices);
        }

        // Validate sizes
        for shard in recovery.iter().flatten() {
            if shard.len() != layout.shard_len {
                return Err(RecoveryError::SizeMismatch {
                    expected: layout.shard_len,
                    actual: shard.len(),
                });
            }
        }

        let decoder = self.get_decoder()?;
        decoder
            .reset(DATA_SLICES, CODING_SLICES, layout.shard_len)
            .map_err(|_| RecoveryError::DecodeFailed)?;

        // Add available shards
        for (i, shard_opt) in recovery.iter().enumerate() {
            if let Some(shard) = shard_opt {
                if i < DATA_SLICES {
                    decoder
                        .add_original_shard(i, shard)
                        .map_err(|_| RecoveryError::DecodeFailed)?;
                } else {
                    decoder
                        .add_recovery_shard(i - DATA_SLICES, shard)
                        .map_err(|_| RecoveryError::DecodeFailed)?;
                }
            }
        }

        let restored = decoder.decode().map_err(|_| RecoveryError::DecodeFailed)?;

        // Reassemble primary from original shards in order
        let mut primary = Vec::with_capacity(layout.padded_len);
        for i in 0..DATA_SLICES {
            let chunk = match recovery[i] {
                Some(data) => data,
                None => restored
                    .restored_original(i)
                    .ok_or(RecoveryError::DecodeFailed)?,
            };
            primary.extend_from_slice(chunk);
        }

        primary.truncate(original_size);
        Ok(primary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};

    fn mk_payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_layout_basic() {
        // shard_len must always be even and non-zero
        let layout = RecoveryLayout::new(0);
        assert!(layout.shard_len >= 2);
        assert_eq!(layout.shard_len % 2, 0);

        let layout = RecoveryLayout::new(1);
        assert_eq!(layout.padded_len % DATA_SLICES, 0);
        assert!(layout.shard_len >= 2);
        assert_eq!(layout.shard_len % 2, 0);
        assert_eq!(layout.shard_len, layout.padded_len / DATA_SLICES);

        let layout = RecoveryLayout::new(10_000);
        assert_eq!(layout.padded_len % DATA_SLICES, 0);
        assert!(layout.shard_len >= 2);
        assert_eq!(layout.shard_len % 2, 0);
    }

    #[test]
    fn test_encode_row_into_matches_encode_shard() {
        let primary = mk_payload(10_000);

        let mut codec = RecoveryCodec::new(primary.len());

        let mut shards: Vec<Vec<u8>> = vec![Vec::new(); SLICE_COUNT];
        codec
            .encode_row_into(&primary, |j, shard| {
                shards[j] = shard.to_vec();
                Ok::<(), RecoveryError>(())
            })
            .unwrap();

        // Ensure all shards are the same size and match codec shard_len
        let shard_len = codec.shard_len();
        for j in 0..SLICE_COUNT {
            assert_eq!(shards[j].len(), shard_len);
        }

        // Spot-check encode_shard agrees for a few indices (data + parity)
        for &j in &[0usize, 1, DATA_SLICES - 1, DATA_SLICES, SLICE_COUNT - 1] {
            let s = codec.encode_shard(&primary, j).unwrap();
            assert_eq!(s, shards[j]);
        }
    }

    #[test]
    fn test_decode_roundtrip_with_minimum_slices() {
        let primary = mk_payload(50_000);

        let mut codec = RecoveryCodec::new(primary.len());

        // Collect shards
        let mut shards: Vec<Vec<u8>> = vec![Vec::new(); SLICE_COUNT];
        codec
            .encode_row_into(&primary, |j, shard| {
                shards[j] = shard.to_vec();
                Ok::<(), RecoveryError>(())
            })
            .unwrap();

        // Keep exactly DATA_SLICES shards:
        // - keep all parity shards (CODING_SLICES)
        // - plus the first (DATA_SLICES - CODING_SLICES) data shards
        let mut keep = vec![false; SLICE_COUNT];
        for j in DATA_SLICES..SLICE_COUNT {
            keep[j] = true;
        }
        let need_data = DATA_SLICES - CODING_SLICES;
        for j in 0..need_data {
            keep[j] = true;
        }

        let present = keep.iter().filter(|b| **b).count();
        assert_eq!(present, DATA_SLICES);

        let recovery: [Option<&[u8]>; SLICE_COUNT] = std::array::from_fn(|j| {
            if keep[j] {
                Some(shards[j].as_slice())
            } else {
                None
            }
        });

        let restored = codec.decode(&recovery, primary.len()).unwrap();
        assert_eq!(restored, primary);
    }

    #[test]
    fn test_decode_not_enough_slices() {
        let primary = mk_payload(10_000);
        let mut codec = RecoveryCodec::new(primary.len());

        let shard_len = codec.shard_len();
        let dummy = vec![0u8; shard_len];

        // Only provide DATA_SLICES-1 shards
        let recovery: [Option<&[u8]>; SLICE_COUNT] = std::array::from_fn(|j| {
            if j < DATA_SLICES - 1 {
                Some(dummy.as_slice())
            } else {
                None
            }
        });

        let err = codec.decode(&recovery, primary.len()).unwrap_err();
        assert!(matches!(err, RecoveryError::NotEnoughSlices));
    }

    #[test]
    fn test_decode_size_mismatch() {
        let primary = mk_payload(10_000);
        let mut codec = RecoveryCodec::new(primary.len());

        // Build one bad shard with the wrong size
        let good_len = codec.shard_len();
        let good = vec![0u8; good_len];
        let bad = vec![0u8; good_len + 1];

        let recovery: [Option<&[u8]>; SLICE_COUNT] = std::array::from_fn(|j| {
            if j == 0 {
                Some(bad.as_slice())
            } else if j < DATA_SLICES {
                Some(good.as_slice())
            } else {
                None
            }
        });

        let err = codec.decode(&recovery, primary.len()).unwrap_err();
        assert!(matches!(err, RecoveryError::SizeMismatch { .. }));
    }
}
