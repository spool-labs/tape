//! Blob encoding for network distribution.
//!
//! This module provides `BlobEncoder` which wraps `BasicSlicer` to encode
//! raw blobs into network-ready slices with merkle commitments.

use tape_slicer::{
    BasicSlicer, Blob, Slicer,
    build_blob_merkle_tree, BlobMerkleRoot,
};

use crate::error::UploadError;

/// Encodes blobs into slices for network distribution.
///
/// Uses Reed-Solomon erasure coding via `BasicSlicer` to produce
/// SLICE_COUNT slices (DATA_SLICES data + CODING_SLICES parity).
pub struct BlobEncoder {
    slicer: BasicSlicer,
}

impl Default for BlobEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobEncoder {
    /// Create a new encoder.
    pub fn new() -> Self {
        Self {
            slicer: BasicSlicer::default(),
        }
    }

    /// Encode a blob into network-ready slices.
    ///
    /// Returns a vector of (slice_index, slice_data) tuples.
    /// The slice index corresponds to the spool where it should be stored.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Vector of (index, data) tuples for all SLICE_COUNT slices.
    pub fn encode(&mut self, data: Vec<u8>) -> Result<Vec<(u16, Vec<u8>)>, UploadError> {
        let blob = Blob::from(data);
        let slices = self.slicer
            .encode(blob)
            .map_err(|e| UploadError::Encoding(e.to_string()))?;

        let output: Vec<(u16, Vec<u8>)> = slices
            .into_iter()
            .map(|slice| (*slice.index as u16, slice.data))
            .collect();

        Ok(output)
    }

    /// Encode and return raw slice data vectors (for uploader compatibility).
    ///
    /// This method returns slices in order (0 to SLICE_COUNT-1), suitable
    /// for passing directly to `DistributedUploader`.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Vector of slice data in index order.
    pub fn encode_to_vec(&mut self, data: Vec<u8>) -> Result<Vec<Vec<u8>>, UploadError> {
        let blob = Blob::from(data);
        let slices = self.slicer
            .encode(blob)
            .map_err(|e| UploadError::Encoding(e.to_string()))?;

        Ok(slices.into_iter().map(|s| s.data).collect())
    }

    /// Encode a blob and compute the Merkle root commitment.
    ///
    /// The Merkle root is used as the blob commitment stored on-chain.
    /// This is the hash that clients use to verify slice integrity.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Tuple of (slices, merkle_root) where slices are (index, data) tuples.
    pub fn encode_with_root(
        &mut self,
        data: Vec<u8>,
    ) -> Result<(Vec<(u16, Vec<u8>)>, BlobMerkleRoot), UploadError> {
        let blob = Blob::from(data);
        let slices = self.slicer
            .encode(blob)
            .map_err(|e| UploadError::Encoding(e.to_string()))?;

        // Build Merkle tree from slices to compute root
        let tree = build_blob_merkle_tree(&slices);
        let root = tree.root();

        let output: Vec<(u16, Vec<u8>)> = slices
            .into_iter()
            .map(|slice| (*slice.index as u16, slice.data))
            .collect();

        Ok((output, root))
    }

    /// Encode a blob with Merkle root, returning raw slice vectors.
    ///
    /// Convenience method combining `encode_to_vec` and merkle root computation.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Tuple of (slice_data_vectors, merkle_root).
    pub fn encode_to_vec_with_root(
        &mut self,
        data: Vec<u8>,
    ) -> Result<(Vec<Vec<u8>>, BlobMerkleRoot), UploadError> {
        let blob = Blob::from(data);
        let slices = self.slicer
            .encode(blob)
            .map_err(|e| UploadError::Encoding(e.to_string()))?;

        // Build Merkle tree from slices
        let tree = build_blob_merkle_tree(&slices);
        let root = tree.root();

        let output: Vec<Vec<u8>> = slices.into_iter().map(|s| s.data).collect();

        Ok((output, root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SLICE_COUNT;

    #[test]
    fn test_encode_basic() {
        let mut encoder = BlobEncoder::new();
        let data = vec![0u8; 10_000];
        let slices = encoder.encode(data).unwrap();

        assert_eq!(slices.len(), SLICE_COUNT);

        // Verify indices are sequential
        for (idx, (slice_idx, _)) in slices.iter().enumerate() {
            assert_eq!(*slice_idx as usize, idx);
        }
    }

    #[test]
    fn test_encode_to_vec() {
        let mut encoder = BlobEncoder::new();
        let data = vec![42u8; 5_000];
        let slices = encoder.encode_to_vec(data).unwrap();

        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_encode_with_root() {
        let mut encoder = BlobEncoder::new();
        let data = vec![0xAB; 20_000];
        let (slices, root) = encoder.encode_with_root(data).unwrap();

        assert_eq!(slices.len(), SLICE_COUNT);

        // Root should be non-zero
        assert_ne!(root.as_ref(), &[0u8; 32]);
    }

    #[test]
    fn test_encode_same_data_same_root() {
        let mut encoder = BlobEncoder::new();
        let data1 = vec![0xCD; 15_000];
        let data2 = data1.clone();

        let (_, root1) = encoder.encode_with_root(data1).unwrap();
        let (_, root2) = encoder.encode_with_root(data2).unwrap();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_encode_different_data_different_root() {
        let mut encoder = BlobEncoder::new();
        let data1 = vec![0xAA; 10_000];
        let data2 = vec![0xBB; 10_000];

        let (_, root1) = encoder.encode_with_root(data1).unwrap();
        let (_, root2) = encoder.encode_with_root(data2).unwrap();

        assert_ne!(root1, root2);
    }

    #[test]
    fn test_encode_empty_blob() {
        let mut encoder = BlobEncoder::new();
        let data = vec![];
        let slices = encoder.encode(data).unwrap();

        // Even empty data produces SLICE_COUNT slices
        assert_eq!(slices.len(), SLICE_COUNT);
    }
}
