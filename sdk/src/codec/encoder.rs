//! Blob encoding for network distribution.
//!
//! This module provides `BlobEncoder` which wraps slicers to encode
//! raw blobs into network-ready slices with merkle commitments.

use tape_core::encoding::{EncodingProfile, EncodingType};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::SpoolIndex;
use tape_crypto::merkle::{create_proof_from_leaf_hashes, hash_leaf, root_from_leaf_hashes};
use tape_crypto::Hash;
use tape_slicer::{
    ClayCoder, ReedSolomonCoder, Slicer, ErasureCoder, SLICE_TREE_HEIGHT,
    build_blob_merkle_tree, BlobMerkleRoot, DEFAULT_STRIPE_SIZE,
};

use crate::error::UploadError;
use crate::transfer::uploader::SliceWithProof;

/// Merkle proof for a single slice.
///
/// Contains SLICE_TREE_HEIGHT sibling hashes needed to verify the slice
/// belongs to a blob with a given merkle root.
pub type SliceMerkleProof = [Hash; SLICE_TREE_HEIGHT];

/// Encodes blobs into slices for network distribution.
///
/// Supports multiple encoding types:
/// - `Basic`: Single RS pass, for testing/debugging only (small blobs)
/// - `Clay`: Clay erasure codes with rotation for fair load distribution (default)
pub struct BlobEncoder {
    profile: EncodingProfile,
    basic: Option<ReedSolomonCoder>,
    clay: Option<Slicer<ClayCoder>>,
}

impl Default for BlobEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobEncoder {
    /// Create a new encoder with default encoding profile (Clay with default params).
    ///
    /// Clay encoding uses MSR erasure codes with per-stripe rotation
    /// for fair load distribution across all nodes.
    pub fn new() -> Self {
        Self::with_profile(EncodingProfile::clay_default())
    }

    /// Create an encoder with a specific encoding profile.
    ///
    /// # Arguments
    /// * `profile` - The encoding profile (type + params)
    pub fn with_profile(profile: EncodingProfile) -> Self {
        let encoding_type = profile.encoding_type().unwrap_or(EncodingType::Unknown);

        let mut encoder = Self {
            profile,
            basic: None,
            clay: None,
        };

        match encoding_type {
            EncodingType::Basic => {
                let params = profile.rs_params();
                encoder.basic = Some(ReedSolomonCoder::new(params.k() as usize, params.m() as usize));
            }
            EncodingType::Clay | EncodingType::Unknown => {
                encoder.clay = Some(Slicer::with_profile(
                    ClayCoder::from_params(profile.clay_params()),
                    DEFAULT_STRIPE_SIZE,
                    true, // rotated
                    profile,
                ));
            }
        }

        encoder
    }

    /// Create an encoder with a specific encoding type (uses default params for that type).
    ///
    /// # Arguments
    /// * `encoding_type` - The encoding algorithm to use
    pub fn with_encoding(encoding_type: EncodingType) -> Self {
        let profile = match encoding_type {
            EncodingType::Basic => EncodingProfile::basic_default(),
            EncodingType::Clay | EncodingType::Unknown => EncodingProfile::clay_default(),
        };
        Self::with_profile(profile)
    }

    /// Get the encoding type used by this encoder.
    pub fn encoding_type(&self) -> EncodingType {
        self.profile.encoding_type().unwrap_or(EncodingType::Unknown)
    }

    /// Get the encoding profile used by this encoder.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }

    /// Internal encoding dispatch that returns the raw chunks.
    fn encode_internal(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, UploadError> {
        match self.encoding_type() {
            EncodingType::Basic => {
                self.basic.as_mut().unwrap()
                    .encode(data)
                    .map_err(|e| UploadError::Encoding(e.to_string()))
            }
            EncodingType::Clay | EncodingType::Unknown => {
                self.clay.as_mut().unwrap()
                    .encode(data)
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
    /// Vector of (index, data) tuples for all GROUP_SIZE slices.
    pub fn encode(&mut self, data: Vec<u8>) -> Result<Vec<(SpoolIndex, Vec<u8>)>, UploadError> {
        let chunks = self.encode_internal(&data)?;

        let output: Vec<(SpoolIndex, Vec<u8>)> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, data)| (i as SpoolIndex, data))
            .collect();

        Ok(output)
    }

    /// Encode and return raw slice data vectors (for uploader compatibility).
    ///
    /// This method returns slices in order (0 to GROUP_SIZE-1), suitable
    /// for passing directly to `DistributedUploader`.
    ///
    /// # Arguments
    /// * `data` - Raw blob data to encode
    ///
    /// # Returns
    /// Vector of slice data in index order.
    pub fn encode_to_vec(&mut self, data: Vec<u8>) -> Result<Vec<Vec<u8>>, UploadError> {
        self.encode_internal(&data)
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
    ) -> Result<(Vec<(SpoolIndex, Vec<u8>)>, BlobMerkleRoot), UploadError> {
        let chunks = self.encode_internal(&data)?;

        // Build Merkle tree from slices to compute root
        let tree = build_blob_merkle_tree(&chunks);
        let root = tree.root();

        let output: Vec<(SpoolIndex, Vec<u8>)> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, data)| (i as SpoolIndex, data))
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
        let chunks = self.encode_internal(&data)?;

        // Build Merkle tree from slices
        let tree = build_blob_merkle_tree(&chunks);
        let root = tree.root();

        Ok((chunks, root))
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
    pub fn encode_with_proofs(
        &mut self,
        data: Vec<u8>,
    ) -> Result<(Vec<SliceWithProof>, BlobMerkleRoot), UploadError> {
        let chunks = self.encode_internal(&data)?;

        // Hash each slice once, then reuse the hashes for the root, proofs,
        // and per-slice leaf hash stored in the upload payload.
        let leaf_hashes: Vec<Hash> = chunks.iter().map(|chunk| hash_leaf(chunk)).collect();
        let root = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaf_hashes);

        let proofs: Result<Vec<Vec<Hash>>, _> = (0..leaf_hashes.len())
            .map(|idx| create_proof_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaf_hashes, idx))
            .collect();
        let proofs = proofs.map_err(|error| UploadError::Encoding(format!("{error:?}")))?;

        // Generate proof for each slice
        let mut output = Vec::with_capacity(chunks.len());
        for (idx, ((chunk, leaf_hash), proof_vec)) in chunks
            .into_iter()
            .zip(leaf_hashes.into_iter())
            .zip(proofs)
            .enumerate()
        {

            // Convert Vec<Hash> to fixed-size array
            let mut proof_arr = [Hash::default(); SLICE_TREE_HEIGHT];
            for (i, h) in proof_vec.into_iter().enumerate() {
                proof_arr[i] = h;
            }

            output.push(SliceWithProof::new(
                idx as SpoolIndex,
                chunk,
                leaf_hash,
                proof_arr,
            ));
        }

        Ok((output, root))
    }

    /// Encode a blob and return slices with proofs, root, and leaf hashes.
    ///
    /// Returns the leaf hashes as a fixed-size array suitable for passing
    /// to the RegisterTrack instruction.
    pub fn encode_with_leaves(
        &mut self,
        data: Vec<u8>,
    ) -> Result<(Vec<SliceWithProof>, BlobMerkleRoot, [Hash; GROUP_SIZE]), UploadError> {
        let (slices, root) = self.encode_with_proofs(data)?;
        let mut leaves = [Hash::default(); GROUP_SIZE];
        for s in &slices {
            leaves[s.index as usize] = s.leaf_hash;
        }
        Ok((slices, root, leaves))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::GROUP_SIZE;

    /// Create a test encoder using ReedSolomonCoder (supports blobs up to ~40 KB).
    fn test_encoder() -> BlobEncoder {
        BlobEncoder::with_encoding(EncodingType::Basic)
    }

    #[test]
    fn test_encode_basic() {
        let mut encoder = test_encoder();
        let data = vec![0u8; 10_000];
        let slices = encoder.encode(data).unwrap();

        assert_eq!(slices.len(), GROUP_SIZE);

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

        assert_eq!(slices.len(), GROUP_SIZE);
    }

    #[test]
    fn test_encode_with_root() {
        let mut encoder = test_encoder();
        let data = vec![0xAB; 20_000];
        let (slices, root) = encoder.encode_with_root(data).unwrap();

        assert_eq!(slices.len(), GROUP_SIZE);

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

        // Even empty data produces GROUP_SIZE slices
        assert_eq!(slices.len(), GROUP_SIZE);
    }

    #[test]
    fn test_encode_with_proofs() {
        use tape_crypto::merkle::verify_proof;

        let mut encoder = test_encoder();
        let data = vec![0x42; 20_000];
        let (slices_with_proofs, root) = encoder.encode_with_proofs(data).unwrap();

        assert_eq!(slices_with_proofs.len(), GROUP_SIZE);

        // Verify each proof
        for slice in &slices_with_proofs {
            let valid = verify_proof(
                &slice.data,
                &root,
                &slice.merkle_proof,
                slice.index as u64,
                SLICE_TREE_HEIGHT,
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
        assert_eq!(encoder.encoding_type(), EncodingType::Clay);
    }

    #[test]
    fn test_encoding_type_basic() {
        let encoder = BlobEncoder::with_encoding(EncodingType::Basic);
        assert_eq!(encoder.encoding_type(), EncodingType::Basic);
    }

    #[test]
    fn test_encoding_type_clay() {
        let encoder = BlobEncoder::with_encoding(EncodingType::Clay);
        assert_eq!(encoder.encoding_type(), EncodingType::Clay);
    }

    #[test]
    fn test_clay_roundtrip_with_decoder() {
        use crate::codec::decoder::BlobDecoder;

        let original = vec![0xAB; 10_000];
        let mut encoder = BlobEncoder::with_encoding(EncodingType::Clay);
        let mut decoder = BlobDecoder::with_encoding(EncodingType::Clay);

        let slices = encoder.encode(original.clone()).unwrap();
        let recovered = decoder.decode(slices).unwrap();

        assert_eq!(original, recovered);
    }
}
