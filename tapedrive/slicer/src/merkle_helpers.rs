use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;
use super::consts::{TOTAL_SLICES, MERKLE_HEIGHT};
use super::types::Shard;

pub type BlobMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;
pub type BlobMerkleRoot = Hash;

pub fn build_blob_merkle_tree(shards: &[Shard; TOTAL_SLICES]) -> BlobMerkleTree {
    let mut tree = BlobMerkleTree::new();
    for s in shards.iter() {
        tree.add_leaf(&s.data).expect("tree capacity");
    }
    tree
}

pub fn blob_merkle_root(shards: &[Shard; TOTAL_SLICES]) -> BlobMerkleRoot {
    build_blob_merkle_tree(shards).root()
}
