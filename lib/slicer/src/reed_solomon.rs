//! Reed-Solomon erasure code wrapper.
//!
//! Provides a thin wrapper around `tape_reed_solomon` with consistent
//! error handling and parameter management.

use tape_reed_solomon::ReedSolomon;

use crate::{ErasureCoder, EncodeError, DecodeError};

/// Byte alignment the kernels work best on, and the floor on a slice.
const SLICE_ALIGN: usize = 64;

/// Reed-Solomon coder (k = data, m = parity).
/// Slice size follows the payload, so one call can encode a whole track.
pub struct ReedSolomonCoder {
    k: usize,
    m: usize,
    rs: ReedSolomon,
}

impl ReedSolomonCoder {
    pub fn new(k: usize, m: usize) -> Self {
        let rs = ReedSolomon::new(k, m).expect("RS init");
        Self { k, m, rs }
    }

    /// Slice length this coder uses for a payload, aligned for the kernel.
    fn slice_bytes(&self, data_len: usize) -> usize {
        data_len.div_ceil(self.k).max(1).div_ceil(SLICE_ALIGN) * SLICE_ALIGN
    }
}

impl ErasureCoder for ReedSolomonCoder {
    #[inline]
    fn k(&self) -> usize {
        self.k
    }

    #[inline]
    fn m(&self) -> usize {
        self.m
    }

    #[inline]
    fn stripe_alignment(&self) -> usize {
        self.k * 64
    }

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        let slice_bytes = self.slice_bytes(data.len());

        let mut slices: Vec<Vec<u8>> = data
            .chunks(slice_bytes)
            .map(|chunk| {
                let mut slice = vec![0u8; slice_bytes];
                slice[..chunk.len()].copy_from_slice(chunk);
                slice
            })
            .collect();
        slices.resize_with(self.n(), || vec![0u8; slice_bytes]);

        self.rs
            .encode(&mut slices)
            .map_err(|_| EncodeError::TooMuchData)?;

        Ok(slices)
    }

    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.len() < self.k {
            return Err(DecodeError::NotEnoughSlices);
        }

        let slice_bytes = chunks[0].1.len();
        let n = self.n();
        if chunks
            .iter()
            .any(|&(idx, data)| idx >= n || data.len() != slice_bytes)
        {
            return Err(DecodeError::InvalidLayout);
        }

        // Reconstruct in place inside the payload buffer, so a data slice is
        // copied once on the way in and never again on the way out.
        let mut payload = vec![0u8; self.k * slice_bytes];
        let mut have_data = vec![false; self.k];
        let mut parity = vec![Vec::new(); self.m];
        for &(idx, data) in chunks {
            match idx.checked_sub(self.k) {
                None => {
                    payload[idx * slice_bytes..][..slice_bytes].copy_from_slice(data);
                    have_data[idx] = true;
                }
                Some(offset) => parity[offset] = data.to_vec(),
            }
        }

        let mut slices: Vec<(&mut [u8], bool)> = payload
            .chunks_mut(slice_bytes)
            .zip(have_data)
            .collect();
        slices.extend(
            parity
                .iter_mut()
                .map(|slice| {
                    let present = !slice.is_empty();
                    (slice.as_mut_slice(), present)
                }),
        );

        self.rs
            .reconstruct_data(&mut slices)
            .map_err(|_| DecodeError::InvalidLayout)?;

        drop(slices);
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: usize = 10;
    const M: usize = 10;

    fn test_coder() -> ReedSolomonCoder {
        ReedSolomonCoder::new(K, M)
    }

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn decode_from(coder: &mut ReedSolomonCoder, chunks: &[Vec<u8>], keep: &[usize]) -> Vec<u8> {
        let kept: Vec<(usize, &[u8])> =
            keep.iter().map(|&i| (i, chunks[i].as_slice())).collect();
        coder.decode(&kept).expect("decode")
    }

    #[test]
    fn round_trips_from_any_k_slices() {
        let sets: [&[usize]; 3] = [
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],           // data only
            &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19], // parity only
            &[0, 2, 4, 6, 8, 11, 13, 15, 17, 19],      // mixed
        ];

        for len in [1_000, 10_000, 4 * 1024 * 1024] {
            let mut coder = test_coder();
            let data = make_data(len);
            let chunks = coder.encode(&data).expect("encode");
            assert_eq!(chunks.len(), K + M);

            for keep in sets {
                let decoded = decode_from(&mut coder, &chunks, keep);
                assert_eq!(&decoded[..data.len()], &data[..], "len={len} keep={keep:?}");
            }
        }
    }

    #[test]
    fn encodes_an_empty_payload() {
        let mut coder = test_coder();
        let chunks = coder.encode(&[]).expect("encode");

        assert_eq!(chunks.len(), K + M);
        assert!(chunks.iter().all(|c| c.len() == SLICE_ALIGN));
    }

    #[test]
    fn too_few_slices_is_rejected() {
        let mut coder = test_coder();
        let data = make_data(1_000);
        let chunks = coder.encode(&data).expect("encode");

        let kept: Vec<(usize, &[u8])> =
            (0..3).map(|i| (i, chunks[i].as_slice())).collect();

        assert!(matches!(
            coder.decode(&kept),
            Err(DecodeError::NotEnoughSlices)
        ));
    }

    #[test]
    fn mismatched_slice_lengths_are_rejected() {
        let mut coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).expect("encode");

        let mut kept: Vec<(usize, &[u8])> =
            (0..K).map(|i| (i, chunks[i].as_slice())).collect();
        kept[3].1 = &chunks[3][..chunks[3].len() - 1];

        assert!(matches!(coder.decode(&kept), Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn an_index_outside_the_group_is_rejected() {
        let mut coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).expect("encode");

        let mut kept: Vec<(usize, &[u8])> =
            (0..K).map(|i| (i, chunks[i].as_slice())).collect();
        kept[0].0 = K + M;

        assert!(matches!(coder.decode(&kept), Err(DecodeError::InvalidLayout)));
    }
}
