use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;
use super::types::Slice;

pub const MERKLE_HEIGHT: usize = 5;

pub type BlobMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;
pub type BlobMerkleRoot = Hash;

/// Build a merkle tree from the slices of an erasure-coded blob.
/// The tree has MERKLE_HEIGHT levels with SPOOL_GROUP_SIZE leaves.
pub fn build_blob_merkle_tree(slices: &[Slice; SPOOL_GROUP_SIZE]) -> BlobMerkleTree {
    let mut tree = BlobMerkleTree::new();
    for s in slices.iter() {
        tree.add_leaf(&s.data).expect("tree capacity");
    }
    tree
}

/// Compute the merkle root (commitment hash) for an erasure-coded blob.
pub fn blob_merkle_root(slices: &[Slice; SPOOL_GROUP_SIZE]) -> BlobMerkleRoot {
    build_blob_merkle_tree(slices).root()
}
