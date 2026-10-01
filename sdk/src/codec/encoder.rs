//! Blob encoding for network distribution.
//!
//! This module provides `BlobEncoder` which wraps slicers to encode
//! raw blobs into network-ready slices with merkle commitments.

use tape_core::encoding::ErasureCode;
use tape_core::erasure::{MAX_GROUP_SIZE, slice_root};
use tape_core::types::SpoolIndex;
use tape_crypto::merkle::MerkleLeafTree;
use tape_crypto::Hash;
use tape_slicer::{
    Slicer, StripedCoder, ErasureCoder, SLICE_TREE_HEIGHT,
    build_blob_merkle_tree, BlobMerkleRoot,
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
/// The network's profile picks the code and its numbers. Every code stripes
/// the blob and rotates slices for fair load distribution across nodes.
pub struct BlobEncoder {
    profile: ErasureCode,
    slicer: Slicer<StripedCoder>,
}

impl BlobEncoder {
    /// Create the encoder for a network's profile.
    pub fn try_new(profile: ErasureCode) -> Result<Self, UploadError> {
        let slicer = Slicer::try_from_profile(profile)
            .map_err(|e| UploadError::Encoding(e.to_string()))?;

        Ok(Self { profile, slicer })
    }

    /// Get the encoding profile used by this encoder.
    pub fn profile(&self) -> ErasureCode {
        self.profile
    }

    /// Stripe size the last encode used; 0 before any encode.
    pub fn stripe_size(&self) -> usize {
        self.slicer.stripe_size()
    }

    /// Internal encoding that returns the raw chunks.
    fn encode_internal(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, UploadError> {
        self.slicer
            .encode(data)
            .map_err(|e| UploadError::Encoding(e.to_string()))
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
    /// Vector of (index, data) tuples for all n slices.
    pub fn encode(&mut self, data: Vec<u8>) -> Result<Vec<(SpoolIndex, Vec<u8>)>, UploadError> {
        let chunks = self.encode_internal(&data)?;

        let output: Vec<(SpoolIndex, Vec<u8>)> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, data)| (SpoolIndex::from(i as u64), data))
            .collect();

        Ok(output)
    }

    /// Encode and return raw slice data vectors (for uploader compatibility).
    ///
    /// This method returns slices in order (0 to n-1), suitable
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
            .map(|(i, data)| (SpoolIndex::from(i as u64), data))
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
        data: &[u8],
    ) -> Result<(Vec<SliceWithProof>, BlobMerkleRoot), UploadError> {
        let chunks = self.encode_internal(data)?;

        // Build each slice's sub-leaf root once, then reuse it for the blob root,
        // the proofs, and the per-slice leaf stored in the upload payload.
        let leaf_hashes: Vec<Hash> = chunks
            .iter()
            .map(|chunk| slice_root(chunk))
            .collect::<Option<Vec<Hash>>>()
            .ok_or_else(|| {
                UploadError::Encoding("slice exceeds sub-leaf tree capacity".to_string())
            })?;
        // One fold serves the root and all n proofs.
        let tree = MerkleLeafTree::new(&leaf_hashes, SLICE_TREE_HEIGHT)
            .map_err(|error| UploadError::Encoding(format!("{error:?}")))?;
        let root = tree.root();

        // Generate proof for each slice
        let mut output = Vec::with_capacity(chunks.len());
        for (idx, (chunk, leaf_hash)) in chunks
            .into_iter()
            .zip(leaf_hashes.iter().copied())
            .enumerate()
        {
            let proof_arr = tree
                .proof_at_n::<SLICE_TREE_HEIGHT>(idx)
                .map_err(|error| UploadError::Encoding(format!("{error:?}")))?;

            output.push(SliceWithProof::new(
                SpoolIndex::from(idx as u64),
                chunk,
                leaf_hash,
                proof_arr,
            )?);
        }

        Ok((output, root))
    }

    /// Encode a blob and return slices with proofs, root, and leaf hashes.
    ///
    /// Returns the leaf hashes as a fixed-size array suitable for passing
    /// to the RegisterTrack instruction. Leaves at or past n stay zero.
    pub fn encode_with_leaves(
        &mut self,
        data: &[u8],
    ) -> Result<(Vec<SliceWithProof>, BlobMerkleRoot, [Hash; MAX_GROUP_SIZE]), UploadError> {
        let (slices, root) = self.encode_with_proofs(data)?;
        let mut leaves = [Hash::default(); MAX_GROUP_SIZE];
        for s in &slices {
            leaves[s.index.as_usize()] = s.leaf_hash;
        }
        Ok((slices, root, leaves))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::{ClayParams, RSParams, RepParams, TAPENET_PROFILE};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::types::{StorageUnits, StripeCount};

    /// A small Reed-Solomon profile: any 4 of 6 slices recover.
    const SMALL: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(4, 2));

    /// Slices the small profile codes a blob into.
    const WIDTH: usize = SMALL.n() as usize;

    fn test_encoder() -> BlobEncoder {
        BlobEncoder::try_new(SMALL).unwrap()
    }

    #[test]
    fn test_encode_basic() {
        let mut encoder = test_encoder();
        let data = vec![0u8; 10_000];
        let slices = encoder.encode(data).unwrap();

        assert_eq!(slices.len(), WIDTH);

        // Verify indices are sequential
        for (idx, (slice_idx, _)) in slices.iter().enumerate() {
            assert_eq!(slice_idx.as_usize(), idx);
        }
    }

    #[test]
    fn test_encode_to_vec() {
        let mut encoder = test_encoder();
        let data = vec![42u8; 5_000];
        let slices = encoder.encode_to_vec(data).unwrap();

        assert_eq!(slices.len(), WIDTH);
    }

    #[test]
    fn test_encode_with_root() {
        let mut encoder = test_encoder();
        let data = vec![0xAB; 20_000];
        let (slices, root) = encoder.encode_with_root(data).unwrap();

        assert_eq!(slices.len(), WIDTH);

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

        // Even empty data produces n slices
        assert_eq!(slices.len(), WIDTH);
    }

    #[test]
    fn test_encode_with_proofs() {
        use tape_crypto::merkle::verify_proof_hash;

        let mut encoder = test_encoder();
        let data = vec![0x42; 20_000];
        let (slices_with_proofs, root) = encoder.encode_with_proofs(&data).unwrap();

        assert_eq!(slices_with_proofs.len(), WIDTH);

        // The top tree commits to slice roots, so the proof starts from the root
        for slice in &slices_with_proofs {
            let valid = verify_proof_hash(
                slice_root(slice.data()).unwrap(),
                &root,
                &slice.merkle_proof,
                slice.index.as_u64(),
                SLICE_TREE_HEIGHT,
            );
            assert!(valid, "Proof verification failed for slice {}", slice.index);
        }
    }

    #[test]
    fn test_encode_with_proofs_indices_sequential() {
        let mut encoder = test_encoder();
        let data = vec![0xAB; 15_000];
        let (slices_with_proofs, _) = encoder.encode_with_proofs(&data).unwrap();

        // Verify indices are sequential
        for (expected_idx, slice) in slices_with_proofs.iter().enumerate() {
            assert_eq!(slice.index.as_usize(), expected_idx);
        }
    }

    #[test]
    fn test_encode_with_proofs_root_matches() {
        let mut encoder = test_encoder();
        let data = vec![0xCD; 20_000];

        // encode_with_root and encode_with_proofs should produce same root
        let (_, root1) = encoder.encode_with_root(data.clone()).unwrap();
        let (_, root2) = encoder.encode_with_proofs(&data).unwrap();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_encode_with_proofs_has_leaf_hash() {
        let mut encoder = test_encoder();
        let data = vec![0xEF; 10_000];
        let (slices_with_proofs, _) = encoder.encode_with_proofs(&data).unwrap();

        // Each leaf is the slice's sub-leaf root, not a hash of the whole slice
        for slice in &slices_with_proofs {
            let expected_leaf = slice_root(slice.data()).unwrap();
            assert_eq!(slice.leaf_hash, expected_leaf);
        }
    }

    /// One profile per code, plus tapenet's.
    fn profiles() -> [ErasureCode; 5] {
        [
            TAPENET_PROFILE,
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            SMALL,
            ErasureCode::Replication(RepParams::new(2)),
        ]
    }

    #[test]
    fn profile_kept() {
        for profile in profiles() {
            let encoder = BlobEncoder::try_new(profile).unwrap();
            assert_eq!(encoder.profile(), profile);
        }
    }

    // a profile no network can have builds no encoder
    #[test]
    fn bad_profile() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(0, 4));
        assert!(BlobEncoder::try_new(profile).is_err());
    }

    // every code writes n slices, leaves the other leaves zero, and commits
    // to the root the chain rebuilds from those leaves
    #[test]
    fn chain_commitment() {
        let data = vec![0x5A; 30_000];

        for profile in profiles() {
            let width = usize::from(profile.n());
            let mut encoder = BlobEncoder::try_new(profile).unwrap();
            let (slices, root, leaves) = encoder.encode_with_leaves(&data).unwrap();

            assert_eq!(slices.len(), width);
            assert!(leaves[..width].iter().all(|leaf| *leaf != Hash::default()));
            assert!(leaves[width..].iter().all(|leaf| *leaf == Hash::default()));

            let blob = BlobEncoding {
                size: StorageUnits::from_bytes(data.len() as u64),
                commitment: root,
                stripe_size: StorageUnits::from_bytes(encoder.stripe_size() as u64),
                stripe_count: StripeCount(1),
                leaves,
            };
            assert_eq!(blob.commitment_root(width), Some(root));
        }
    }

    #[test]
    fn test_roundtrip_with_decoder() {
        use crate::codec::decoder::BlobDecoder;

        let original = vec![0xAB; 10_000];

        for profile in profiles() {
            let mut encoder = BlobEncoder::try_new(profile).unwrap();
            let decoder = BlobDecoder::try_new(profile).unwrap();

            let slices = encoder.encode(original.clone()).unwrap();
            let refs: Vec<(SpoolIndex, &[u8])> = slices.iter().map(|(idx, data)| (*idx, data.as_slice())).collect();
            let recovered = decoder.decode(&refs).unwrap();

            assert_eq!(original, recovered);
        }
    }
}
