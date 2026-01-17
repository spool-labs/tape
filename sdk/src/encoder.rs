//! Blob encoding for network distribution.
//!
//! This module provides `BlobEncoder` which wraps slicers to encode
//! raw blobs into network-ready slices with merkle commitments.

use tape_core::prelude::EncodingType;
use tape_crypto::merkle::{create_merkle_proof, hash_leaf};
use tape_crypto::Hash;
use tape_slicer::{
    BasicSlicer, StripedSlicer, RotatedSlicer, Blob, Slicer, Slice, MERKLE_HEIGHT, SLICE_COUNT,
    build_blob_merkle_tree, BlobMerkleRoot,
};

use crate::error::UploadError;
use crate::uploader::SliceWithProof;

/// Merkle proof for a single slice.
///
/// Contains MERKLE_HEIGHT sibling hashes needed to verify the slice
/// belongs to a blob with a given merkle root.
pub type SliceMerkleProof = [Hash; MERKLE_HEIGHT];

/// Encodes blobs into slices for network distribution.
///
/// Supports multiple encoding types:
/// - `Basic`: Single RS pass, for testing/debugging only
/// - `Striped`: Multiple stripes with fixed slice assignment, production-ready
/// - `Rotated`: Striped with per-stripe rotation for fair load distribution (default)
pub struct BlobEncoder {
    encoding_type: EncodingType,
    basic: Option<BasicSlicer>,
    striped: Option<StripedSlicer>,
    rotated: Option<RotatedSlicer>,
}

impl Default for BlobEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobEncoder {
    /// Create a new encoder with default encoding type (Rotated).
    ///
    /// Rotated encoding provides fair load distribution across all nodes
    /// and is the recommended default for production use.
    pub fn new() -> Self {
        Self::with_encoding(EncodingType::Rotated)
    }

    /// Create an encoder with a specific encoding type.
    ///
    /// # Arguments
    /// * `encoding_type` - The encoding algorithm to use
    pub fn with_encoding(encoding_type: EncodingType) -> Self {
        let mut encoder = Self {
            encoding_type,
            basic: None,
            striped: None,
            rotated: None,
        };

        match encoding_type {
            EncodingType::Basic => {
                encoder.basic = Some(BasicSlicer::default());
            }
            EncodingType::Striped => {
                encoder.striped = Some(StripedSlicer::default());
            }
            EncodingType::Rotated | EncodingType::Unknown => {
                encoder.rotated = Some(RotatedSlicer::default());
            }
        }

        encoder
    }

    /// Create an encoder with a custom max slice size (for BasicSlicer only).
    ///
    /// Use smaller values for testing to reduce memory usage.
    /// For production, use `new()` or `with_encoding()`.
    pub fn with_max_slice_bytes(max_slice_bytes: usize) -> Self {
        Self {
            encoding_type: EncodingType::Basic,
            basic: Some(BasicSlicer::with_max_slice_bytes(max_slice_bytes)),
            striped: None,
            rotated: None,
        }
    }

    /// Get the encoding type used by this encoder.
    pub fn encoding_type(&self) -> EncodingType {
        self.encoding_type
    }

    /// Internal encoding dispatch that returns the raw Slice array.
    fn encode_internal(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], UploadError> {
        match self.encoding_type {
            EncodingType::Basic => {
                self.basic.as_mut().unwrap()
                    .encode(blob)
                    .map_err(|e| UploadError::Encoding(e.to_string()))
            }
            EncodingType::Striped => {
                self.striped.as_mut().unwrap()
                    .encode(blob)
                    .map_err(|e| UploadError::Encoding(e.to_string()))
            }
            EncodingType::Rotated | EncodingType::Unknown => {
                self.rotated.as_mut().unwrap()
                    .encode(blob)
                    .map_err(|e| UploadError::Encoding(e.to_string()))
            }
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
        let slices = self.encode_internal(blob)?;

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
        let slices = self.encode_internal(blob)?;

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
        let slices = self.encode_internal(blob)?;

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
        let slices = self.encode_internal(blob)?;

        // Build Merkle tree from slices
        let tree = build_blob_merkle_tree(&slices);
        let root = tree.root();

        let output: Vec<Vec<u8>> = slices.into_iter().map(|s| s.data).collect();

        Ok((output, root))
    }

    /// Encode a blob and generate merkle proofs for each slice.
    ///
    /// This is the full encoding method needed for uploading to storage nodes.
    /// Each slice includes a merkle proof that allows the storage node to verify
    /// that the slice belongs to the claimed blob.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Tuple containing:
    /// - Vector of `SliceWithProof` (index, data, leaf_hash, merkle_proof)
    /// - The blob merkle root (commitment)
    ///
    /// # Example
    /// ```ignore
    /// let mut encoder = BlobEncoder::new();
    /// let (slices_with_proofs, root) = encoder.encode_with_proofs(data)?;
    ///
    /// for slice in slices_with_proofs {
    ///     // Upload slice to node responsible for spool slice.index
    ///     // Node can verify: verify_proof(&slice.data, &root, &slice.merkle_proof, slice.index, MERKLE_HEIGHT)
    /// }
    /// ```
    pub fn encode_with_proofs(
        &mut self,
        data: Vec<u8>,
    ) -> Result<(Vec<SliceWithProof>, BlobMerkleRoot), UploadError> {
        let blob = Blob::from(data);
        let slices = self.encode_internal(blob)?;

        // Build Merkle tree from slices
        let tree = build_blob_merkle_tree(&slices);
        let root = tree.root();

        // Collect slice data for proof generation (need owned copies for lifetime)
        let slice_data_owned: Vec<Vec<u8>> = slices.iter().map(|s| s.data.clone()).collect();
        let slice_data_refs: Vec<&[u8]> = slice_data_owned.iter().map(|s| s.as_slice()).collect();

        // Generate proof for each slice
        let mut output = Vec::with_capacity(slices.len());
        for (idx, slice) in slices.into_iter().enumerate() {
            let proof_vec = create_merkle_proof(&slice_data_refs, idx, MERKLE_HEIGHT);

            // Convert Vec<Hash> to fixed-size array
            let mut proof_arr = [Hash::default(); MERKLE_HEIGHT];
            for (i, h) in proof_vec.into_iter().enumerate() {
                proof_arr[i] = h;
            }

            // Compute leaf hash for this slice
            let leaf_hash = hash_leaf(&slice.data);

            output.push(SliceWithProof::new(
                *slice.index as u16,
                slice.data,
                leaf_hash,
                proof_arr,
            ));
        }

        Ok((output, root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SLICE_COUNT;

    /// Smaller max slice size for testing to reduce memory usage.
    /// 4 KiB allows encoding blobs up to ~2.7 MB (DATA_SLICES * 4 KiB).
    const TEST_MAX_SLICE_BYTES: usize = 1 << 12; // 4 KiB

    /// Create a test encoder with reduced memory footprint.
    fn test_encoder() -> BlobEncoder {
        BlobEncoder::with_max_slice_bytes(TEST_MAX_SLICE_BYTES)
    }

    #[test]
    fn test_encode_basic() {
        let mut encoder = test_encoder();
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
        let mut encoder = test_encoder();
        let data = vec![42u8; 5_000];
        let slices = encoder.encode_to_vec(data).unwrap();

        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_encode_with_root() {
        let mut encoder = test_encoder();
        let data = vec![0xAB; 20_000];
        let (slices, root) = encoder.encode_with_root(data).unwrap();

        assert_eq!(slices.len(), SLICE_COUNT);

        // Root should be non-zero
        assert_ne!(root.as_ref(), &[0u8; 32]);
    }

    #[test]
    fn test_encode_same_data_same_root() {
        let mut encoder = test_encoder();
        let data1 = vec![0xCD; 15_000];
        let data2 = data1.clone();

        let (_, root1) = encoder.encode_with_root(data1).unwrap();
        let (_, root2) = encoder.encode_with_root(data2).unwrap();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_encode_different_data_different_root() {
        let mut encoder = test_encoder();
        let data1 = vec![0xAA; 10_000];
        let data2 = vec![0xBB; 10_000];

        let (_, root1) = encoder.encode_with_root(data1).unwrap();
        let (_, root2) = encoder.encode_with_root(data2).unwrap();

        assert_ne!(root1, root2);
    }

    #[test]
    fn test_encode_empty_blob() {
        let mut encoder = test_encoder();
        let data = vec![];
        let slices = encoder.encode(data).unwrap();

        // Even empty data produces SLICE_COUNT slices
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_encode_with_proofs() {
        use tape_crypto::merkle::verify_proof;

        let mut encoder = test_encoder();
        let data = vec![0x42; 30_000];
        let (slices_with_proofs, root) = encoder.encode_with_proofs(data).unwrap();

        assert_eq!(slices_with_proofs.len(), SLICE_COUNT);

        // Verify each proof
        for slice in &slices_with_proofs {
            let valid = verify_proof(
                &slice.data,
                &root,
                &slice.merkle_proof,
                slice.index as u64,
                MERKLE_HEIGHT,
            );
            assert!(valid, "Proof verification failed for slice {}", slice.index);
        }
    }

    #[test]
    fn test_encode_with_proofs_indices_sequential() {
        let mut encoder = test_encoder();
        let data = vec![0xAB; 15_000];
        let (slices_with_proofs, _) = encoder.encode_with_proofs(data).unwrap();

        // Verify indices are sequential
        for (expected_idx, slice) in slices_with_proofs.iter().enumerate() {
            assert_eq!(slice.index as usize, expected_idx);
        }
    }

    #[test]
    fn test_encode_with_proofs_root_matches() {
        let mut encoder = test_encoder();
        let data = vec![0xCD; 20_000];

        // encode_with_root and encode_with_proofs should produce same root
        let (_, root1) = encoder.encode_with_root(data.clone()).unwrap();
        let (_, root2) = encoder.encode_with_proofs(data).unwrap();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_encode_with_proofs_has_leaf_hash() {
        use tape_crypto::merkle::hash_leaf;

        let mut encoder = test_encoder();
        let data = vec![0xEF; 10_000];
        let (slices_with_proofs, _) = encoder.encode_with_proofs(data).unwrap();

        // Verify leaf hashes are correctly computed
        for slice in &slices_with_proofs {
            let expected_leaf = hash_leaf(&slice.data);
            assert_eq!(slice.leaf_hash, expected_leaf);
        }
    }

    #[test]
    fn test_encoding_type_default() {
        let encoder = BlobEncoder::new();
        assert_eq!(encoder.encoding_type(), EncodingType::Rotated);
    }

    #[test]
    fn test_encoding_type_basic() {
        let encoder = BlobEncoder::with_encoding(EncodingType::Basic);
        assert_eq!(encoder.encoding_type(), EncodingType::Basic);
    }

    #[test]
    fn test_encoding_type_striped() {
        let encoder = BlobEncoder::with_encoding(EncodingType::Striped);
        assert_eq!(encoder.encoding_type(), EncodingType::Striped);
    }

    #[test]
    fn test_encoding_type_rotated() {
        let encoder = BlobEncoder::with_encoding(EncodingType::Rotated);
        assert_eq!(encoder.encoding_type(), EncodingType::Rotated);
    }

    #[test]
    fn test_striped_roundtrip_with_decoder() {
        use crate::decoder::BlobDecoder;

        let original = vec![0xAB; 10_000];
        let mut encoder = BlobEncoder::with_encoding(EncodingType::Striped);
        let mut decoder = BlobDecoder::with_encoding(EncodingType::Striped);

        let slices = encoder.encode(original.clone()).unwrap();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_rotated_roundtrip_with_decoder() {
        use crate::decoder::BlobDecoder;

        let original = vec![0xCD; 10_000];
        let mut encoder = BlobEncoder::with_encoding(EncodingType::Rotated);
        let mut decoder = BlobDecoder::with_encoding(EncodingType::Rotated);

        let slices = encoder.encode(original.clone()).unwrap();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }
}
