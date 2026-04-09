//! Merkle tree helpers for blob commitments.
//!
//! Two distinct trees live in `BlobInfo`:
//!
//! - `commitment` is a tree over the **erasure-coded slice leaves** — see
//!   [`build_blob_merkle_tree`] and [`blob_merkle_root`]. It serves the
//!   storage layer (per-slice membership proofs).
//! - `root` is a tree over the **stripes of the source data** — see
//!   [`source_root`], [`source_proof`], and [`verify_source_proof`]. It
//!   serves the application layer (stripe inclusion proofs against
//!   `track.value_hash` without touching erasure slices).
//!
//! See `lib/core/src/track/blob.rs` for the design rationale.

use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE, STRIPE_TREE_HEIGHT};
use tape_crypto::Hash;
use tape_crypto::merkle::{
    MerkleError, MerkleTree, create_proof_from_leaf_hashes, hash_leaf, root_from_leaf_hashes,
    verify_proof,
};

use crate::adaptive::num_stripes;

pub const MERKLE_HEIGHT: usize = COMMITMENT_TREE_HEIGHT;

pub type BlobMerkleTree = MerkleTree<{ COMMITMENT_TREE_HEIGHT }>;
pub type BlobMerkleRoot = Hash;

/// Build a merkle tree from the slices of an erasure-coded blob.
/// The tree has MERKLE_HEIGHT levels with SPOOL_GROUP_SIZE leaves.
///
/// Accepts any slice-like data that can be converted to `&[u8]`.
pub fn build_blob_merkle_tree<T: AsRef<[u8]>>(slices: &[T]) -> BlobMerkleTree {
    assert!(
        slices.len() <= SPOOL_GROUP_SIZE,
        "too many slices for merkle tree"
    );
    let mut tree = BlobMerkleTree::new();
    for s in slices.iter() {
        tree.add_leaf(s.as_ref()).expect("tree capacity");
    }
    tree
}

/// Compute the merkle root (commitment hash) for an erasure-coded blob.
pub fn blob_merkle_root<T: AsRef<[u8]>>(slices: &[T]) -> BlobMerkleRoot {
    build_blob_merkle_tree(slices).root()
}

/// Hash each stripe of `data` at `stripe_size` boundaries.
///
/// Returns one leaf hash per stripe, in stripe-index order. Used by both
/// [`source_root`] and [`source_proof`] to keep the leaf computation
/// consistent.
fn stripe_leaves(data: &[u8], stripe_size: usize) -> Vec<Hash> {
    let n = num_stripes(data.len(), stripe_size);
    let mut leaves = Vec::with_capacity(n);
    for s in 0..n {
        let start = s * stripe_size;
        let end = (start + stripe_size).min(data.len());
        leaves.push(hash_leaf(&data[start..end]));
    }
    leaves
}

/// Compute the source-data merkle root: a tree over per-stripe leaf hashes.
///
/// The leaves are `hash_leaf(stripe[i])` for each stripe of `data` cut at
/// `stripe_size`. Empty trailing slots fill out to `STRIPE_TREE_HEIGHT`
/// capacity using the precomputed empty-subtree roots.
///
/// `stripe_size` must match the value the encoder used (and stored in
/// `BlobInfo.stripe_size`); otherwise the resulting root will not match the
/// canonical commitment for the blob. Callers that use the slicer should
/// take `stripe_size` from `Slicer::stripe_size()` *after* calling
/// `encode`, since the slicer may auto-adjust via `pick_stripe_size`.
pub fn source_root(data: &[u8], stripe_size: usize) -> Hash {
    let leaves = stripe_leaves(data, stripe_size);
    root_from_leaf_hashes::<STRIPE_TREE_HEIGHT>(&leaves)
}

/// Build a stripe inclusion proof for the stripe at `index`.
///
/// The returned proof is a path of `STRIPE_TREE_HEIGHT` sibling hashes
/// suitable for [`verify_source_proof`]. Returns an error if `index` is
/// out of range for the data's stripe count.
pub fn source_proof(
    data: &[u8],
    stripe_size: usize,
    index: usize,
) -> Result<Vec<Hash>, MerkleError> {
    let leaves = stripe_leaves(data, stripe_size);
    create_proof_from_leaf_hashes::<STRIPE_TREE_HEIGHT>(&leaves, index)
}

/// Verify a stripe inclusion proof against a source-data merkle root.
///
/// `stripe_data` is the raw stripe bytes (the same slice the encoder fed
/// into the inner coder). `index` is the stripe's position. `proof` is a
/// path of `STRIPE_TREE_HEIGHT` sibling hashes from [`source_proof`]. The
/// `root` is read from `BlobInfo.root` (which is itself committed to by
/// `track.value_hash`).
pub fn verify_source_proof(
    stripe_data: &[u8],
    root: &Hash,
    proof: &[Hash],
    index: u64,
) -> bool {
    verify_proof(stripe_data, root, proof, index, STRIPE_TREE_HEIGHT)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Single-leaf tree for empty data (num_stripes returns 1 for empty input).
    #[test]
    fn source_root_empty_data() {
        let root_a = source_root(&[], 100_000);
        let root_b = source_root(&[], 100_000);
        assert_eq!(root_a, root_b);

        // Manually compute the expected single-empty-leaf root at the configured height.
        let leaf = hash_leaf(&[]);
        let expected = root_from_leaf_hashes::<STRIPE_TREE_HEIGHT>(&[leaf]);
        assert_eq!(root_a, expected);
    }

    // Data smaller than stripe_size produces a one-leaf tree.
    #[test]
    fn source_root_single_stripe() {
        let data = vec![0xABu8; 50_000];
        let root = source_root(&data, 100_000);

        let leaf = hash_leaf(&data);
        let expected = root_from_leaf_hashes::<STRIPE_TREE_HEIGHT>(&[leaf]);
        assert_eq!(root, expected);
    }

    // Multi-stripe data: leaves match a manual MerkleTree::<STRIPE_TREE_HEIGHT> build.
    #[test]
    fn source_root_multiple_stripes() {
        let stripe_size = 1_000;
        // 5 full stripes + 1 partial
        let data: Vec<u8> = (0..5_300u32).map(|i| (i % 251) as u8).collect();

        let root = source_root(&data, stripe_size);

        // Manual build via MerkleTree directly
        let mut tree = MerkleTree::<STRIPE_TREE_HEIGHT>::new();
        for s in 0..6 {
            let start = s * stripe_size;
            let end = (start + stripe_size).min(data.len());
            tree.add_leaf(&data[start..end]).expect("capacity");
        }
        assert_eq!(root, tree.root());
    }

    // Roundtrip: build a proof for a stripe, verify it; flip a byte, verify rejects.
    #[test]
    fn source_proof_roundtrip() {
        let stripe_size = 1_000;
        let data: Vec<u8> = (0..5_000u32).map(|i| (i % 251) as u8).collect();

        let root = source_root(&data, stripe_size);
        let target_index: usize = 2;
        let proof = source_proof(&data, stripe_size, target_index).expect("proof");

        let target_start = target_index * stripe_size;
        let target_end = (target_start + stripe_size).min(data.len());
        let stripe = &data[target_start..target_end];

        assert!(verify_source_proof(stripe, &root, &proof, target_index as u64));

        // Tamper one byte and confirm rejection.
        let mut tampered = stripe.to_vec();
        tampered[0] ^= 0xFF;
        assert!(!verify_source_proof(&tampered, &root, &proof, target_index as u64));

        // Wrong index should also reject.
        assert!(!verify_source_proof(stripe, &root, &proof, (target_index as u64) + 1));
    }

    // Out-of-range proof index returns an error rather than an empty proof.
    #[test]
    fn source_proof_out_of_range() {
        let data = vec![0u8; 1_500];
        let result = source_proof(&data, 1_000, 99);
        assert!(result.is_err());
    }

    // Different stripe sizes for the same data produce different roots
    // (because the leaf granularity changes).
    #[test]
    fn source_root_depends_on_stripe_size() {
        let data = vec![0xCDu8; 10_000];
        let root_1k = source_root(&data, 1_000);
        let root_5k = source_root(&data, 5_000);
        assert_ne!(root_1k, root_5k);
    }
}
