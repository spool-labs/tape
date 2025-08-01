use steel::*;
use crate::consts::*;
use brine_tree::MerkleTree;

pub type SegmentTree = MerkleTree<{SEGMENT_TREE_HEIGHT}>;
pub type TapeTree = MerkleTree<{TAPE_TREE_HEIGHT}>;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
/// Proof-of-work solution needed to mine a block using CrankX
pub struct PoW {
    pub digest: [u8; 16],
    pub nonce: [u8; 8],
}

impl PoW {
    pub fn as_solution(&self) -> crankx::Solution {
        crankx::Solution::new(self.digest, self.nonce)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
/// Proof-of-access solution for the tape segment, cryptographically tied to the miner using PackX.
pub struct PoA {
    pub bump: [u8; 8],
    pub seed: [u8; 16],
    pub nonce: [u8; 128],
    pub path: [[u8; 32]; SEGMENT_PROOF_LEN],
}

impl PoA {
    pub fn as_solution(&self) -> packx::Solution {
        packx::Solution::new(self.seed, self.nonce, self.bump)
    }
}
