use steel::*;
use crate::consts::*;
use crate::state;
use super::AccountType;
use brine_tree::MerkleTree;

pub type TapeTree = MerkleTree<{TREE_HEIGHT}>;

#[repr(C, align(8))] 
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Writer {
    pub tape: Pubkey,
    pub state: TapeTree, 
}

state!(AccountType, Writer);
