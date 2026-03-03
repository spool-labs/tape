//! Clay erasure code wrapper.
//!
//! Provides a thin wrapper around `clay_codes::ClayCode` with consistent
//! error handling and parameter management.

use std::collections::{HashMap, HashSet};
use clay_codes::ClayCode;
use tape_core::encoding::ClayParams;

use crate::{ErasureCoder, EncodeError, DecodeError};

/// Clay erasure code wrapper (k = data, m = parity, d = helper count).
pub struct ClayCoder {
    pub clay: ClayCode,
    pub k: usize,
    pub m: usize,
    pub d: usize,
}

impl ClayCoder {
    /// Create a new Clay coder with the given parameters.
    ///
    /// Matches `ClayParams::new(n, k, d)` parameter order.
    pub fn new(n: usize, k: usize, d: usize) -> Self {
        assert!(n > k, "n must be > k");
        assert!(k > 0, "k must be > 0");
        let m = n - k;
        assert!(d >= k + 1, "d must be >= k + 1");
        assert!(d <= n - 1, "d must be <= n - 1");

        let clay = ClayCode::new(k, m, d).expect("Clay code init");

        Self { clay, k, m, d }
    }

    /// Create from ClayParams.
    pub fn from_params(params: ClayParams) -> Self {
        Self::new(params.n() as usize, params.k() as usize, params.d() as usize)
    }

    /// Helper count (d).
    #[inline]
    pub fn d(&self) -> usize {
        self.d
    }

    /// Sub-chunks per chunk (α = q^t).
    #[inline]
    pub fn alpha(&self) -> usize {
        self.clay.sub_chunk_no
    }

    /// Sub-chunks per helper during repair (β = α/q).
    #[inline]
    pub fn beta(&self) -> usize {
        self.clay.beta
    }

    /// Compute the chunk size for a given input length (deterministic, no encoding needed).
    ///
    /// Mirrors the padding logic in clay_codes::encode: input is padded to
    /// align to k × alpha × 2, then chunk_size = padded_len / k.
    pub fn chunk_size_for(&self, input_len: usize) -> usize {
        let alpha = self.alpha();
        let min_size = self.k * alpha * 2;
        let padded_len = if input_len == 0 {
            min_size
        } else {
            let aligned = ((input_len + min_size - 1) / min_size) * min_size;
            aligned.max(min_size)
        };
        padded_len / self.k
    }

    /// Compute the encoded chunk size for a track with the given stripe_size and blob_len.
    ///
    /// Uses `min(stripe_size, blob_len)` as the effective input length to handle
    /// small blobs where actual data is less than a full stripe. This is the
    /// single source of truth for chunk_size computation — used by both the
    /// repair planner and the repair HTTP handler.
    pub fn track_chunk_size(&self, stripe_size: usize, blob_len: usize) -> usize {
        let effective_len = stripe_size.min(blob_len);
        self.chunk_size_for(effective_len)
    }

}

impl ErasureCoder for ClayCoder {
    #[inline]
    fn k(&self) -> usize {
        self.k
    }

    #[inline]
    fn m(&self) -> usize {
        self.m
    }

    fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::EmptyInput);
        }
        Ok(self.clay.encode(data))
    }

    fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.len() < self.k {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Build HashMap + erasures for clay-codes library
        let available: HashMap<usize, Vec<u8>> = chunks
            .iter()
            .map(|(idx, data)| (*idx, data.to_vec()))
            .collect();
        let present: HashSet<usize> = chunks.iter().map(|(idx, _)| *idx).collect();
        let erasures: Vec<usize> = (0..self.n()).filter(|i| !present.contains(i)).collect();

        self.clay
            .decode(&available, &erasures)
            .map_err(|_| DecodeError::BadEncoding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_coder() -> ClayCoder {
        ClayCoder::new(20, 10, 19)
    }

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_params() {
        let coder = test_coder();
        assert_eq!(coder.k(), 10);
        assert_eq!(coder.m(), 10);
        assert_eq!(coder.n(), 20);
        assert_eq!(coder.d(), 19);
    }

    #[test]
    fn test_from_params() {
        let params = ClayParams::new(20, 10, 19);
        let coder = ClayCoder::from_params(params);
        assert_eq!(coder.k(), 10);
        assert_eq!(coder.m(), 10);
        assert_eq!(coder.n(), 20);
    }

    #[test]
    fn test_chunk_count() {
        let mut coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).unwrap();
        assert_eq!(chunks.len(), coder.n());
    }

    #[test]
    fn test_uniform_chunks() {
        let mut coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).unwrap();
        let size = chunks[0].len();
        assert!(chunks.iter().all(|c| c.len() == size));
    }

    #[test]
    fn test_roundtrip_all() {
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_data_only() {
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only first k chunks (data chunks)
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(coder.k())
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_parity_only() {
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only last k chunks (parity chunks, indices k..n)
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(coder.k())
            .take(coder.k())
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_mixed_chunks() {
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep every other chunk
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_insufficient() {
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only k-1 chunks
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(coder.k() - 1)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let result = coder.decode(&available);
        assert!(matches!(result, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_empty_fails() {
        let mut coder = test_coder();
        let result = coder.encode(&[]);
        assert!(matches!(result, Err(EncodeError::EmptyInput)));
    }

    #[test]
    fn test_chunk_size_for() {
        let mut coder = test_coder();

        // Verify that chunk_size_for matches actual encoding output
        for len in [100, 1000, 2000, 10_000, 100_000, 1_000_000] {
            let data = make_data(len);
            let chunks = coder.encode(&data).unwrap();
            let actual_chunk_size = chunks[0].len();
            let predicted = coder.chunk_size_for(len);
            assert_eq!(
                predicted, actual_chunk_size,
                "chunk_size_for({len}) = {predicted}, actual = {actual_chunk_size}"
            );
        }
    }

    #[test]
    fn test_chunk_size_for_min() {
        let coder = test_coder();
        // Even for very small inputs, chunk_size must be at least alpha * 2
        let min_chunk = coder.chunk_size_for(1);
        assert!(min_chunk >= coder.alpha() * 2);
    }

}
