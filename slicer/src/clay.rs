//! Clay erasure code wrapper.
//!
//! Provides a thin wrapper around `clay_codes::ClayCode` with consistent
//! error handling and parameter management, similar to `reed_solomon.rs`.

use std::collections::HashMap;
use clay_codes::ClayCode;
use tape_core::encoding::ClayParams;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ClayEncodeError {
    #[error("input data is empty")]
    EmptyInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ClayDecodeError {
    #[error("not enough chunks to reconstruct (need at least k)")]
    NotEnoughChunks,
    #[error("decoding failed")]
    DecodeFailed,
}

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

    /// Data chunks (k) needed for reconstruction.
    #[inline]
    pub fn k(&self) -> usize {
        self.k
    }

    /// Parity chunks (m).
    #[inline]
    pub fn m(&self) -> usize {
        self.m
    }

    /// Total chunks (n = k + m).
    #[inline]
    pub fn n(&self) -> usize {
        self.k + self.m
    }

    /// Helper count (d).
    #[inline]
    pub fn d(&self) -> usize {
        self.d
    }

    /// Encode data into n chunks.
    ///
    /// Returns a vector of n chunks. The Clay code handles internal padding.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, ClayEncodeError> {
        if data.is_empty() {
            return Err(ClayEncodeError::EmptyInput);
        }
        Ok(self.clay.encode(data))
    }

    /// Decode from available chunks.
    ///
    /// # Arguments
    /// * `available` - Map of chunk_index -> chunk_data for available chunks
    /// * `erasures` - List of missing chunk indices
    ///
    /// # Returns
    /// The reconstructed original data.
    pub fn decode(
        &self,
        available: &HashMap<usize, Vec<u8>>,
        erasures: &[usize],
    ) -> Result<Vec<u8>, ClayDecodeError> {
        if available.len() < self.k {
            return Err(ClayDecodeError::NotEnoughChunks);
        }

        self.clay
            .decode(available, erasures)
            .map_err(|_| ClayDecodeError::DecodeFailed)
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
    fn test_encode_chunk_count() {
        let coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).unwrap();
        assert_eq!(chunks.len(), coder.n());
    }

    #[test]
    fn test_encode_uniform_chunk_size() {
        let coder = test_coder();
        let data = make_data(10_000);
        let chunks = coder.encode(&data).unwrap();
        let size = chunks[0].len();
        assert!(chunks.iter().all(|c| c.len() == size));
    }

    #[test]
    fn test_roundtrip_all_chunks() {
        let coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        let available: HashMap<usize, Vec<u8>> = chunks
            .into_iter()
            .enumerate()
            .collect();
        let erasures: Vec<usize> = vec![];

        let recovered = coder.decode(&available, &erasures).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_roundtrip_data_only() {
        let coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only first k chunks (data chunks)
        let available: HashMap<usize, Vec<u8>> = chunks
            .into_iter()
            .enumerate()
            .take(coder.k())
            .collect();
        let erasures: Vec<usize> = (coder.k()..coder.n()).collect();

        let recovered = coder.decode(&available, &erasures).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_roundtrip_parity_only() {
        let coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only last k chunks (parity chunks, indices k..n)
        let available: HashMap<usize, Vec<u8>> = chunks
            .into_iter()
            .enumerate()
            .skip(coder.k())
            .take(coder.k())
            .collect();
        let erasures: Vec<usize> = (0..coder.k()).collect();

        let recovered = coder.decode(&available, &erasures).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_roundtrip_mixed() {
        let coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep every other chunk
        let available: HashMap<usize, Vec<u8>> = chunks
            .into_iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .collect();
        let erasures: Vec<usize> = (0..coder.n()).filter(|i| i % 2 != 0).collect();

        let recovered = coder.decode(&available, &erasures).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_not_enough_chunks() {
        let coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only k-1 chunks
        let available: HashMap<usize, Vec<u8>> = chunks
            .into_iter()
            .enumerate()
            .take(coder.k() - 1)
            .collect();
        let erasures: Vec<usize> = (coder.k() - 1..coder.n()).collect();

        let result = coder.decode(&available, &erasures);
        assert!(matches!(result, Err(ClayDecodeError::NotEnoughChunks)));
    }

    #[test]
    fn test_encode_empty_fails() {
        let coder = test_coder();
        let result = coder.encode(&[]);
        assert!(matches!(result, Err(ClayEncodeError::EmptyInput)));
    }

}
