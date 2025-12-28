use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;
use super::consts::{SLICE_COUNT, MERKLE_HEIGHT};
use super::types::Shard;

pub type BlobMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;
pub type BlobMerkleRoot = Hash;

pub fn build_blob_merkle_tree(shards: &[Shard; SLICE_COUNT]) -> BlobMerkleTree {
    let mut tree = BlobMerkleTree::new();
    for s in shards.iter() {
        tree.add_leaf(&s.data).expect("tree capacity");
    }
    tree
}

pub fn blob_merkle_root(shards: &[Shard; SLICE_COUNT]) -> BlobMerkleRoot {
    build_blob_merkle_tree(shards).root()
}
