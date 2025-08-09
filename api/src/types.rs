use steel::*;
use core::ops::{Deref, Index};
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
    pub fn from_solution(solution: &crankx::Solution) -> Self {
        Self {
            digest: solution.d,
            nonce: solution.n,
        }
    }

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
    pub path: ProofPath,
}

impl PoA {
    pub fn from_solution(solution: &packx::Solution, path: impl Into<ProofPath>) -> Self {
        Self {
            bump:  solution.bump,
            seed:  solution.seeds,
            nonce: solution.nonces,
            path:  path.into(),
        }
    }

    pub fn as_solution(&self) -> packx::Solution {
        packx::Solution::new(self.seed, self.nonce, self.bump)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct ProofPath(pub [[u8; 32]; SEGMENT_PROOF_LEN]);

// Safe: transparent wrapper over a byte-array-of-arrays.
unsafe impl Zeroable for ProofPath {}
unsafe impl Pod for ProofPath {}

impl ProofPath {
    /// Construct from an array
    pub fn from_array(path: [[u8; 32]; SEGMENT_PROOF_LEN]) -> Self {
        Self(path)
    }

    /// Lossless extract of the inner array by value.
    pub fn into_array(self) -> [[u8; 32]; SEGMENT_PROOF_LEN] {
        self.0
    }

    /// Borrow the inner array.
    pub fn as_array(&self) -> &[[u8; 32]; SEGMENT_PROOF_LEN] {
        &self.0
    }

    /// Mutable borrow of the inner array.
    pub fn as_mut_array(&mut self) -> &mut [[u8; 32]; SEGMENT_PROOF_LEN] {
        &mut self.0
    }

    /// Try to build from a slice; returns None if length != SEGMENT_PROOF_LEN.
    pub fn from_slice(slice: &[[u8; 32]]) -> Option<Self> {
        <[[u8; 32]; SEGMENT_PROOF_LEN]>::try_from(slice).ok().map(Self)
    }

    /// Iterator over the 32-byte nodes.
    pub fn iter(&self) -> core::slice::Iter<'_, [u8; 32]> {
        self.0.iter()
    }
}

impl From<[[u8; 32]; SEGMENT_PROOF_LEN]> for ProofPath {
    fn from(path: [[u8; 32]; SEGMENT_PROOF_LEN]) -> Self {
        Self::from_array(path)
    }
}

impl AsRef<[[u8; 32]; SEGMENT_PROOF_LEN]> for ProofPath {
    fn as_ref(&self) -> &[[u8; 32]; SEGMENT_PROOF_LEN] {
        self.as_array()
    }
}

impl Deref for ProofPath {
    type Target = [[u8; 32]; SEGMENT_PROOF_LEN];
    fn deref(&self) -> &Self::Target {
        self.as_array()
    }
}

impl Index<usize> for ProofPath {
    type Output = [u8; 32];
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl Default for ProofPath {
    fn default() -> Self {
        <Self as Zeroable>::zeroed()
    }
}
