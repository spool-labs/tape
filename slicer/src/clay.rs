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
    clay: ClayCode,
    k: usize,
    m: usize,
    d: usize,
}

impl ClayCoder {
    /// Create a new Clay coder with the given parameters.
    pub fn new(k: usize, m: usize, d: usize) -> Self {
        assert!(k > 0, "k must be > 0");
        assert!(m > 0, "m must be > 0");
        assert!(d >= k + 1, "d must be >= k + 1");
        assert!(d <= k + m - 1, "d must be <= n - 1");

        let clay = ClayCode::new(k, m, d).expect("Clay code init");

        Self { clay, k, m, d }
    }

    /// Create from ClayParams.
    pub fn from_params(params: ClayParams) -> Self {
        Self::new(params.k() as usize, params.m() as usize, params.d() as usize)
    }

    /// Helper count (d).
    #[inline]
    pub fn d(&self) -> usize {
        self.d
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
        ClayCoder::new(10, 10, 19)
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
}
