//! Merkle tree helpers for blob commitments.

use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;

pub const MERKLE_HEIGHT: usize = 5;

pub type BlobMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;
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
