//! Outer Reed-Solomon code for snapshot erasure across spool groups.
//!
//! The outer code distributes snapshot data across the active spool groups,
//! providing high fault tolerance. This is a single-level RS code, no
//! striping or rotation. `n` (the active group count) is supplied at
//! construction by the caller.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::errors::{DecodeError, EncodeError};

/// Default number of data chunks for outer RS code.
/// At n=50 this gives 72% group failure tolerance with k=14.
pub const DEFAULT_K_OUTER: usize = 14;

/// Outer RS `k` used by the snapshot pipeline.
/// At n=50 this gives ~1/3 recovery threshold (17/50).
pub const SNAPSHOT_K_OUTER: usize = 17;

/// Maximum per-symbol size for the outer RS coder, set by the
/// `reed_solomon_simd` shard-size constraint.
pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Outer Reed-Solomon coder for snapshot distribution across spool groups.
///
/// Encodes data into `n` chunks (one per spool group) such that any `k`
/// chunks suffice to reconstruct the original data. Unlike the inner
/// slicer, this does NOT use striping or rotation.
pub struct OuterCoder {
    k: usize,
    n: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl OuterCoder {
    /// Create a new outer coder with the given k (data chunks) and
    /// n (total chunks = active spool group count).
    pub fn new(k: usize, n: usize) -> Self {
        assert!(k > 0, "k must be > 0");
        assert!(k <= n, "k must be <= n");

        let m = n - k;
        let encoder =
            ReedSolomonEncoder::new(k, m, MAX_CHUNK_BYTES).expect("RS encoder init");
        let decoder =
            ReedSolomonDecoder::new(k, m, MAX_CHUNK_BYTES).expect("RS decoder init");

        Self {
            k,
            n,
            encoder,
            decoder,
        }
    }

    /// Number of data chunks needed for reconstruction.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Total number of output chunks (= SPOOL_GROUP_COUNT).
    pub fn n(&self) -> usize {
        self.n
    }

    /// Parity chunks (n - k).
    pub fn m(&self) -> usize {
        self.n - self.k
    }

    /// Encode data into n chunks (one per spool group).
    ///
    /// Returns a Vec of n chunks. The first k are data chunks,
    /// the remaining m are parity chunks.
    pub fn encode(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, EncodeError> {
        let k = self.k;
        let m = self.m();

        // Calculate chunk size: ceil(data.len() / k), 64-byte aligned
        let chunk_bytes = if data.is_empty() {
            64
        } else {
            let raw = (data.len() + k - 1) / k;
            ((raw + 63) / 64) * 64
        };

        if chunk_bytes > MAX_CHUNK_BYTES {
            return Err(EncodeError::TooMuchData);
        }

        self.encoder
            .reset(k, m, chunk_bytes)
            .map_err(|_| EncodeError::TooMuchData)?;

        // Pad data to k * chunk_bytes
        let total_len = k * chunk_bytes;
        let mut padded = data.to_vec();
        padded.resize(total_len, 0);

        // Feed k original chunks
        let mut data_chunks = Vec::with_capacity(k);
        for chunk in padded.chunks(chunk_bytes) {
            self.encoder
                .add_original_shard(chunk)
                .expect("adding chunks of the configured size should succeed");
            data_chunks.push(chunk.to_vec());
        }

        // Generate parity chunks
        let output = self
            .encoder
            .encode()
            .expect("should be able to encode after k data chunks were added");
        let parity_chunks: Vec<Vec<u8>> = output.recovery_iter().map(<[u8]>::to_vec).collect();

        let mut result = data_chunks;
        result.extend(parity_chunks);
        Ok(result)
    }

    /// Decode from at least k chunks.
    ///
    /// Input: (chunk_index, chunk_data) pairs where chunk_index is 0..n-1.
    /// Returns the reconstructed original data (may have trailing padding).
    pub fn decode(&mut self, chunks: &[(usize, &[u8])]) -> Result<Vec<u8>, DecodeError> {
        if chunks.len() < self.k {
            return Err(DecodeError::NotEnoughSlices);
        }

        let chunk_bytes = chunks
            .first()
            .map(|(_, data)| data.len())
            .ok_or(DecodeError::InvalidLayout)?;

        if chunks.iter().any(|(_, data)| data.len() != chunk_bytes) {
            return Err(DecodeError::InvalidLayout);
        }

        let m = self.m();

        self.decoder
            .reset(self.k, m, chunk_bytes)
            .map_err(|_| DecodeError::TooMuchData)?;

        for &(idx, data) in chunks {
            if idx < self.k {
                self.decoder
                    .add_original_shard(idx, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else if idx < self.n {
                let offset = idx - self.k;
                self.decoder
                    .add_recovery_shard(offset, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let restored = self
            .decoder
            .decode()
            .map_err(|_| DecodeError::InvalidLayout)?;

        // Reassemble payload from data chunks in order [0..k)
        let provided: std::collections::HashSet<usize> =
            chunks.iter().map(|(i, _)| *i).collect();
        let chunks_map: std::collections::HashMap<usize, &[u8]> =
            chunks.iter().map(|&(i, d)| (i, d)).collect();

        let mut payload = Vec::with_capacity(self.k * chunk_bytes);
        for data_idx in 0..self.k {
            let slice_ref = if provided.contains(&data_idx) {
                *chunks_map.get(&data_idx).unwrap()
            } else {
                restored
                    .restored_original(data_idx)
                    .ok_or(DecodeError::InvalidLayout)?
            };
            payload.extend_from_slice(slice_ref);
        }

        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPOOL_GROUP_COUNT: usize = 50;

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_chunk_count() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let data = make_data(100_000);
        let chunks = coder.encode(&data).unwrap();

        assert_eq!(chunks.len(), SPOOL_GROUP_COUNT);
        assert_eq!(coder.k(), DEFAULT_K_OUTER);
        assert_eq!(coder.n(), SPOOL_GROUP_COUNT);
        assert_eq!(coder.m(), SPOOL_GROUP_COUNT - DEFAULT_K_OUTER);

        // All chunks should be the same size
        let size = chunks[0].len();
        assert!(chunks.iter().all(|c| c.len() == size));
    }

    #[test]
    fn test_roundtrip_all_chunks() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_data_chunks_only() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only first k chunks (data chunks)
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(DEFAULT_K_OUTER)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_parity_chunks_only() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only parity chunks (last m = 36 chunks), take k of them
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(DEFAULT_K_OUTER)
            .take(DEFAULT_K_OUTER)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_mixed_chunks() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep every 3rd chunk: 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 3 == 0)
            .take(DEFAULT_K_OUTER)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        assert!(available.len() >= DEFAULT_K_OUTER);
        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_insufficient_chunks() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only k-1 chunks
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(DEFAULT_K_OUTER - 1)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let result = coder.decode(&available);
        assert!(matches!(result, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_empty_data() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let chunks = coder.encode(&[]).unwrap();
        assert_eq!(chunks.len(), SPOOL_GROUP_COUNT);

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();
        let recovered = coder.decode(&available).unwrap();
        assert!(recovered.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_various_sizes() {
        let mut coder = OuterCoder::new(DEFAULT_K_OUTER, SPOOL_GROUP_COUNT);
        let sizes = [1, 13, DEFAULT_K_OUTER, 1000, 50_000, 200_000];

        for &sz in &sizes {
            let original = make_data(sz);
            let chunks = coder.encode(&original).unwrap();

            let available: Vec<(usize, &[u8])> = chunks
                .iter()
                .enumerate()
                .take(DEFAULT_K_OUTER)
                .map(|(i, c)| (i, c.as_slice()))
                .collect();

            let recovered = coder.decode(&available).unwrap();
            assert_eq!(
                &recovered[..original.len()],
                &original,
                "roundtrip failed for size {}",
                sz
            );
        }
    }

    #[test]
    fn test_custom_k() {
        // Test with k=7 (matching inner Clay k)
        let mut coder = OuterCoder::new(7, SPOOL_GROUP_COUNT);
        let original = make_data(50_000);
        let chunks = coder.encode(&original).unwrap();
        assert_eq!(chunks.len(), SPOOL_GROUP_COUNT);

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(7)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }
}
