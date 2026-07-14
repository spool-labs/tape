//! Single-level Reed-Solomon coder distributing data across `n` shards such
//! that any `k` reconstruct it. No striping or rotation; `n` (e.g. the active
//! spool group count) is supplied at construction. Snapshot-specific `k`/segment
//! sizing lives in `tape-snapshot`.

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::errors::{DecodeError, EncodeError};

/// Maximum per-symbol size for the outer RS coder, set by the
/// `reed_solomon_simd` shard-size constraint.
pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Outer Reed-Solomon coder for distribution across `n` shards.
///
/// Encodes data into `n` chunks (one per spool group) such that any `k`
/// chunks suffice to reconstruct the original data. Unlike the inner
/// slicer, this does NOT use striping or rotation.
pub struct OuterCoder {
    k: usize,
    n: usize,
    encoder: Option<ReedSolomonEncoder>,
    decoder: Option<ReedSolomonDecoder>,
}

impl OuterCoder {
    /// Create a new outer coder with the given k (data chunks) and
    /// n (total chunks = active spool group count).
    pub fn new(k: usize, n: usize) -> Self {
        assert!(k > 0, "k must be > 0");
        assert!(k <= n, "k must be <= n");

        let m = n - k;
        let (encoder, decoder) = if m == 0 {
            (None, None)
        } else {
            (
                Some(ReedSolomonEncoder::new(k, m, MAX_CHUNK_BYTES).expect("RS encoder init")),
                Some(ReedSolomonDecoder::new(k, m, MAX_CHUNK_BYTES).expect("RS decoder init")),
            )
        };

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

        // Pad data to k * chunk_bytes
        let total_len = k * chunk_bytes;
        let mut padded = data.to_vec();
        padded.resize(total_len, 0);

        // Feed k original chunks
        let mut data_chunks = Vec::with_capacity(k);
        for chunk in padded.chunks(chunk_bytes) {
            data_chunks.push(chunk.to_vec());
        }

        let Some(encoder) = self.encoder.as_mut() else {
            return Ok(data_chunks);
        };

        encoder
            .reset(k, m, chunk_bytes)
            .map_err(|_| EncodeError::TooMuchData)?;

        for chunk in &data_chunks {
            encoder
                .add_original_shard(chunk)
                .expect("adding chunks of the configured size should succeed");
        }

        // Generate parity chunks
        let output = encoder
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

        if m == 0 {
            let mut payload = Vec::with_capacity(self.k * chunk_bytes);
            for data_idx in 0..self.k {
                let Some((_, data)) = chunks.iter().find(|(idx, _)| *idx == data_idx) else {
                    return Err(DecodeError::InvalidLayout);
                };
                payload.extend_from_slice(data);
            }
            return Ok(payload);
        }

        let decoder = self.decoder.as_mut().ok_or(DecodeError::InvalidLayout)?;
        decoder
            .reset(self.k, m, chunk_bytes)
            .map_err(|_| DecodeError::TooMuchData)?;

        for &(idx, data) in chunks {
            if idx < self.k {
                decoder
                    .add_original_shard(idx, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else if idx < self.n {
                let offset = idx - self.k;
                decoder
                    .add_recovery_shard(offset, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            } else {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let restored = decoder
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
    const TEST_K: usize = 17; // ceil(50 / 3)

    fn make_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_chunk_count() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let data = make_data(100_000);
        let chunks = coder.encode(&data).unwrap();

        assert_eq!(chunks.len(), SPOOL_GROUP_COUNT);
        assert_eq!(coder.k(), TEST_K);
        assert_eq!(coder.n(), SPOOL_GROUP_COUNT);
        assert_eq!(coder.m(), SPOOL_GROUP_COUNT - TEST_K);

        // All chunks should be the same size
        let size = chunks[0].len();
        assert!(chunks.iter().all(|c| c.len() == size));
    }

    #[test]
    fn single_group_roundtrip_has_no_parity() {
        let mut coder = OuterCoder::new(1, 1);
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(coder.k(), 1);
        assert_eq!(coder.m(), 0);

        let available = [(0, chunks[0].as_slice())];
        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_roundtrip_all_chunks() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
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
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only first k chunks (data chunks)
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(TEST_K)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_parity_chunks_only() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only parity chunks (last m = 36 chunks), take k of them
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .skip(TEST_K)
            .take(TEST_K)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_decode_with_mixed_chunks() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let original = make_data(100_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep every 3rd chunk: 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 3 == 0)
            .take(TEST_K)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        assert!(available.len() >= TEST_K);
        let recovered = coder.decode(&available).unwrap();
        assert_eq!(&recovered[..original.len()], &original);
    }

    #[test]
    fn test_insufficient_chunks() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let original = make_data(10_000);
        let chunks = coder.encode(&original).unwrap();

        // Keep only k-1 chunks
        let available: Vec<(usize, &[u8])> = chunks
            .iter()
            .enumerate()
            .take(TEST_K - 1)
            .map(|(i, c)| (i, c.as_slice()))
            .collect();

        let result = coder.decode(&available);
        assert!(matches!(result, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_empty_data() {
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
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
        let mut coder = OuterCoder::new(TEST_K, SPOOL_GROUP_COUNT);
        let sizes = [1, 13, TEST_K, 1000, 50_000, 200_000];

        for &sz in &sizes {
            let original = make_data(sz);
            let chunks = coder.encode(&original).unwrap();

            let available: Vec<(usize, &[u8])> = chunks
                .iter()
                .enumerate()
                .take(TEST_K)
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
