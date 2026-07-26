//! Reed-Solomon erasure code wrapper.
//!
//! Provides a thin wrapper around `tape_reed_solomon` with consistent
//! error handling and parameter management.

use tape_reed_solomon::ReedSolomon;

use crate::{ErasureCoder, EncodeError, DecodeError};

/// Reed-Solomon coder (k = data, m = parity).
/// A thin wrapper around tape-reed-solomon. Slice size follows the payload, so
/// there is no ceiling on how much data one call may encode.
pub struct ReedSolomonCoder {
    k: usize,
    m: usize,
    rs: ReedSolomon,
}

impl ReedSolomonCoder {
    /// Create a new Reed-Solomon coder.
    pub fn new(k: usize, m: usize) -> Self {
        assert!(k > 0, "k must be > 0");
        assert!(m > 0, "m must be > 0");

        let n = k + m;
        assert!(n <= 65536, "too many total slices for RS field");

        let rs = ReedSolomon::new(k, m).expect("RS init");

        Self { k, m, rs }
    }

    /// Slice length this coder uses for a payload, aligned for the kernel.
    fn slice_bytes(&self, data_len: usize) -> usize {
        if data_len == 0 {
            64
        } else {
            data_len.div_ceil(self.k).div_ceil(64) * 64
        }
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

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        let slice_bytes = self.slice_bytes(data.len());

        // Copy the payload straight into its data slices, zero-padding the tail.
        // Parity slices start zeroed and are filled in place.
        let mut slices: Vec<Vec<u8>> = Vec::with_capacity(self.k + self.m);
        for i in 0..self.k {
            let mut slice = vec![0u8; slice_bytes];
            let start = (i * slice_bytes).min(data.len());
            let end = (start + slice_bytes).min(data.len());
            slice[..end - start].copy_from_slice(&data[start..end]);
            slices.push(slice);
        }
        slices.resize(self.k + self.m, vec![0u8; slice_bytes]);

        self.rs
            .encode(&mut slices)
            .map_err(|_| EncodeError::TooMuchData)?;

        Ok(slices)
    }

    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.len() < self.k {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Infer slice_bytes from any present chunk
        let slice_bytes = chunks
            .first()
            .map(|(_, data)| data.len())
            .ok_or(DecodeError::InvalidLayout)?;

        // Ensure all chunks have the same size
        if chunks.iter().any(|(_, data)| data.len() != slice_bytes) {
            return Err(DecodeError::InvalidLayout);
        }

        let n = self.k + self.m;
        let mut slices: Vec<Option<Vec<u8>>> = vec![None; n];
        for &(idx, data) in chunks {
            if idx >= n {
                return Err(DecodeError::InvalidLayout);
            }
            slices[idx] = Some(data.to_vec());
        }

        self.rs
            .reconstruct_data(&mut slices)
            .map_err(|_| DecodeError::InvalidLayout)?;

        // Reassemble the payload from data slices in order [0..k)
        let mut payload = Vec::with_capacity(self.k * slice_bytes);
        for slice in slices.iter().take(self.k) {
            payload.extend_from_slice(slice.as_ref().ok_or(DecodeError::InvalidLayout)?);
        }

        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test constants (k=10, m=10, n=20)
    const K: usize = 10;
    const M: usize = 10;

    fn test_coder() -> ReedSolomonCoder {
        ReedSolomonCoder::new(K, M)
    }

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn keep_indices(chunks: &[Vec<u8>], keep: &[usize]) -> Vec<(usize, Vec<u8>)> {
        keep.iter().map(|&i| (i, chunks[i].clone())).collect()
    }

    #[test]
    fn round_trip_all_slices() {
        let mut coder = test_coder();
        let data = make_data(10_000);

        let chunks = coder.encode(&data).expect("encode");
        assert_eq!(chunks.len(), K + M);

        let borrowed: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(K)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();
        let decoded = coder.decode(&borrowed).expect("decode");

        assert_eq!(&decoded[..data.len()], &data[..]);
    }

    #[test]
    fn round_trip_from_parity_only() {
        let mut coder = test_coder();
        let data = make_data(10_000);

        let chunks = coder.encode(&data).expect("encode");
        let kept = keep_indices(&chunks, &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
        let borrowed: Vec<(usize, &[u8])> =
            kept.iter().map(|(i, c)| (*i, c.as_slice())).collect();

        let decoded = coder.decode(&borrowed).expect("decode");
        assert_eq!(&decoded[..data.len()], &data[..]);
    }

    #[test]
    fn round_trip_mixed_slices() {
        let mut coder = test_coder();
        let data = make_data(10_000);

        let chunks = coder.encode(&data).expect("encode");
        let kept = keep_indices(&chunks, &[0, 2, 4, 6, 8, 11, 13, 15, 17, 19]);
        let borrowed: Vec<(usize, &[u8])> =
            kept.iter().map(|(i, c)| (*i, c.as_slice())).collect();

        let decoded = coder.decode(&borrowed).expect("decode");
        assert_eq!(&decoded[..data.len()], &data[..]);
    }

    #[test]
    fn too_few_slices_is_rejected() {
        let mut coder = test_coder();
        let data = make_data(1_000);

        let chunks = coder.encode(&data).expect("encode");
        let kept = keep_indices(&chunks, &[0, 1, 2]);
        let borrowed: Vec<(usize, &[u8])> =
            kept.iter().map(|(i, c)| (*i, c.as_slice())).collect();

        assert!(matches!(
            coder.decode(&borrowed),
            Err(DecodeError::NotEnoughSlices)
        ));
    }

    #[test]
    fn encodes_a_payload_far_over_the_old_four_kib_cap() {
        let mut coder = test_coder();
        let data = make_data(4 * 1024 * 1024);

        let chunks = coder.encode(&data).expect("encode");
        assert_eq!(chunks.len(), K + M);
        assert!(chunks[0].len() > 4096);
    }
}
