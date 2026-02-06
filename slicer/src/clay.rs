//! Clay erasure code wrapper.
//!
//! Provides a thin wrapper around `clay_codes::ClayCode` with consistent
//! error handling and parameter management.

use std::collections::{HashMap, HashSet};
use clay_codes::ClayCode;
use tape_core::encoding::ClayParams;

use crate::{ErasureCoder, EncodeError, DecodeError};
use crate::errors::RepairError;
use crate::SliceIndex;

/// Clay erasure code wrapper (k = data, m = parity, d = helper count).
pub struct ClayCoder {
    clay: ClayCode,
    k: usize,
    m: usize,
    d: usize,
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

    /// Sub-chunks per chunk (α = q^t). For default params (k=10, m=10, d=19): 100.
    #[inline]
    pub fn alpha(&self) -> usize {
        self.clay.sub_chunk_no
    }

    /// Sub-chunks per helper during repair (β = α/q). For default params: 10.
    #[inline]
    pub fn beta(&self) -> usize {
        self.clay.beta
    }

    /// Compute repair plan for a single lost shard.
    ///
    /// Returns `(helper_shard, sub_chunk_indices)` per helper.
    pub fn plan_repair(
        &self,
        lost: SliceIndex,
        available: &[SliceIndex],
    ) -> Result<Vec<(SliceIndex, Vec<u32>)>, RepairError> {
        let avail: Vec<usize> = available.iter().map(|s| **s).collect();
        let helpers = self.clay.minimum_to_repair(*lost, &avail).map_err(|e| {
            RepairError::Clay(e.to_string())
        })?;
        helpers
            .into_iter()
            .map(|(idx, sub_chunks)| {
                let si = SliceIndex::new(idx).ok_or(RepairError::InvalidSlice)?;
                Ok((si, sub_chunks.into_iter().map(|v| v as u32).collect()))
            })
            .collect()
    }

    /// Repair a single lost shard from partial helper data.
    ///
    /// `helpers`: shard_idx → concatenated sub-chunks (order from `plan_repair`).
    pub fn repair(
        &self,
        lost: SliceIndex,
        helpers: &HashMap<SliceIndex, Vec<u8>>,
        chunk_size: usize,
    ) -> Result<Vec<u8>, RepairError> {
        let helper_data: HashMap<usize, Vec<u8>> = helpers
            .iter()
            .map(|(idx, data)| (**idx, data.clone()))
            .collect();
        self.clay
            .repair(*lost, &helper_data, chunk_size)
            .map_err(|e| RepairError::Clay(e.to_string()))
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
    fn repair_coder_direct() {
        // ClayCoder plan_repair + repair on a single chunk (no striping)
        let mut coder = test_coder();
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();
        let chunk_size = chunks[0].len();
        let alpha = coder.alpha();
        let sub_chunk_size = chunk_size / alpha;

        for lost_idx in [0, 5, 19] {
            let lost = SliceIndex::new(lost_idx).unwrap();
            let available: Vec<SliceIndex> = (0..20)
                .filter(|&i| i != lost_idx)
                .map(|i| SliceIndex::new(i).unwrap())
                .collect();

            let plan = coder.plan_repair(lost, &available).unwrap();
            assert_eq!(plan.len(), coder.d());

            // Extract sub-chunks per the plan
            let mut helpers: HashMap<SliceIndex, Vec<u8>> = HashMap::new();
            for (helper_si, sub_indices) in &plan {
                let mut partial = Vec::new();
                for &sc in sub_indices {
                    let start = sc as usize * sub_chunk_size;
                    let end = start + sub_chunk_size;
                    partial.extend_from_slice(&chunks[**helper_si][start..end]);
                }
                helpers.insert(*helper_si, partial);
            }

            let recovered = coder.repair(lost, &helpers, chunk_size).unwrap();
            assert_eq!(recovered, chunks[lost_idx], "repair failed for shard {lost_idx}");
        }
    }
}
