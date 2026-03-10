//! Reed-Solomon erasure code wrapper.
//!
//! Provides a thin wrapper around `reed_solomon_simd` with consistent
//! error handling and parameter management.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::{ErasureCoder, EncodeError, DecodeError};

/// Maximum slice size for ReedSolomonCoder (used for testing/debugging only).
/// 4 KiB allows encoding blobs up to ~40 KB (k * 4 KiB with k=10).
/// For production workloads, use Slicer which handles large blobs efficiently.
pub const MAX_SLICE_BYTES: usize = 1 << 12; // 4 KiB

/// Reed-Solomon coder (k = data, m = parity).
/// This is a thin wrapper around reed_solomon_simd. It reuses working buffers across calls.
pub struct ReedSolomonCoder {
    k: usize,
    m: usize,
    max_slice_bytes: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl ReedSolomonCoder {
    /// Create a new Reed-Solomon coder with default max slice size (4 KiB).
    /// This is suitable for testing/debugging. For larger blobs, use `with_max_slice_bytes`.
    pub fn new(k: usize, m: usize) -> Self {
        Self::with_max_slice_bytes(k, m, MAX_SLICE_BYTES)
    }

    /// Create a new Reed-Solomon coder with a custom max slice size.
    ///
    /// The max_slice_bytes determines the maximum size of each slice,
    /// which affects memory allocation in the encoder/decoder.
    /// Use larger values for benchmarks or when encoding large blobs.
    pub fn with_max_slice_bytes(k: usize, m: usize, max_slice_bytes: usize) -> Self {
        assert!(k > 0, "k must be > 0");
        assert!(m > 0, "m must be > 0");
        assert!(max_slice_bytes > 0, "max_slice_bytes must be > 0");

        let n = k + m;
        assert!(n <= 65536, "too many total slices for RS field");

        // Use a bounded max slice size the library accepts. Per-call reset() will set the actual slice size.
        let encoder = ReedSolomonEncoder::new(k, m, max_slice_bytes)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(k, m, max_slice_bytes)
            .expect("RS decoder init");

        Self {
            k,
            m,
            max_slice_bytes,
            encoder,
            decoder,
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
        let k = self.k;

        // Calculate slice size: ceil(data.len() / k)
        // Must be at least 1 byte, and we round up to 64-byte alignment for RS efficiency
        let slice_bytes = if data.is_empty() {
            64 // Minimal aligned slice for empty data
        } else {
            let raw = (data.len() + k - 1) / k;
            // RS library works best with 64-byte aligned slices
            ((raw + 63) / 64) * 64
        };

        // Ensure the encoder can handle this slice size
        if slice_bytes > self.max_slice_bytes {
            return Err(EncodeError::TooMuchData);
        }

        self.encoder
            .reset(self.k, self.m, slice_bytes)
            .map_err(|_| EncodeError::TooMuchData)?;

        // Pad data to k * slice_bytes
        let total_len = k * slice_bytes;
        let mut padded = data.to_vec();
        padded.resize(total_len, 0);

        // Feed k original slices into the encoder
        let mut data_chunks = Vec::with_capacity(k);
        for chunk in padded.chunks(slice_bytes) {
            self.encoder
                .add_original_shard(chunk)
                .expect("adding slices of the configured size should succeed");
            data_chunks.push(chunk.to_vec());
        }

        // Create parity slices
        let output = self
            .encoder
            .encode()
            .expect("should be able to encode after k data slices were added");
        let coding_chunks: Vec<Vec<u8>> = output.recovery_iter().map(<[u8]>::to_vec).collect();

        // Return all chunks: data first, then parity
        let mut result = data_chunks;
        result.extend(coding_chunks);
        Ok(result)
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

        self.decoder
            .reset(self.k, self.m, slice_bytes)
            .map_err(|_| DecodeError::TooMuchData)?;

        // Feed chunks into decoder based on their indices
        for &(idx, data) in chunks {
            if idx < self.k {
                // Data chunk (original)
                self.decoder
                    .add_original_shard(idx, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else if idx < self.k + self.m {
                // Parity chunk (recovery)
                let offset = idx - self.k;
                self.decoder
                    .add_recovery_shard(offset, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let restored = self.decoder.decode().map_err(|_| DecodeError::InvalidLayout)?;

        // Reassemble the payload from data slices in order [0..k)
        // Build a set of provided indices for quick lookup
        let provided: std::collections::HashSet<usize> = chunks.iter().map(|(i, _)| *i).collect();
        let chunks_map: std::collections::HashMap<usize, &[u8]> =
            chunks.iter().map(|&(i, d)| (i, d)).collect();

        let mut payload = Vec::with_capacity(self.k * slice_bytes);
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

    // Test constants (k=10, m=10, n=20)
    const K: usize = 10;
    const M: usize = 10;

    fn test_coder() -> ReedSolomonCoder {
        ReedSolomonCoder::new(K, M)
    }

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[allow(dead_code)]
    fn keep_indices(chunks: &[Vec<u8>], keep: &[usize]) -> Vec<(usize, Vec<u8>)> {
        keep.iter()
            .filter_map(|&i| chunks.get(i).map(|c| (i, c.clone())))
            .collect()
    }

    #[test]
    fn test_chunk_count() {
        let mut coder = test_coder();
        let data = make_data(20_000);
        let chunks = coder.encode(&data).unwrap();

        assert_eq!(chunks.len(), K + M);

        // All chunks should be the same size
        let size = chunks[0].len();
        assert!(chunks.iter().all(|c| c.len() == size));
    }

    #[test]
    fn test_roundtrip_all() {
        let mut coder = test_coder();
        let original = make_data(20_000);
        let chunks = coder.encode(&original).unwrap();

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        // Note: recovered may have padding at end
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_data_only() {
        let mut coder = test_coder();
        let original = make_data(20_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only first k chunks (data chunks)
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(K)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_mixed_chunks() {
        let mut coder = test_coder();
        let original = make_data(20_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep every other chunk (0, 2, 4, 6, 8, 10, 12, 14, 16, 18) = 10 chunks
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
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
            .take(K - 1)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let result = coder.decode(&available);
        assert!(matches!(result, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_empty() {
        let mut coder = test_coder();
        let chunks = coder.encode(&[]).unwrap();
        assert_eq!(chunks.len(), K + M);

        // Roundtrip
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();
        let recovered = coder.decode(&available).unwrap();
        // Empty data decodes to k 1-byte zero chunks
        assert!(recovered.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_size_mismatch() {
        let mut coder = test_coder();
        let original = make_data(20_000);
        let mut chunks = coder.encode(&original).unwrap();

        // Corrupt one chunk by truncating it
        chunks[0].pop();

        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let result = coder.decode(&available);
        assert!(matches!(result, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn test_many_sizes() {
        let mut coder = test_coder();
        let sizes = [1, K - 1, K, K + 1, 2 * K, 5_000, 20_000, 30_000];

        for &sz in &sizes {
            let original = make_data(sz);
            let chunks = coder.encode(&original).unwrap();

            let available: Vec<(usize, &[u8])> = chunks
                .iter()
                .enumerate()
                .take(K)
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
}
