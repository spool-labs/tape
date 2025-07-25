use crate::consts::*;
use brine_tree::MerkleTree;

pub type SegmentTree = MerkleTree<{SEGMENT_TREE_HEIGHT}>;
pub type TapeTree = MerkleTree<{TAPE_TREE_HEIGHT}>;
