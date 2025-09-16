use steel::*;
use super::AccountType;
use crate::{state, types::BlockNumber, hash::Hash};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Block {
    pub number: BlockNumber,

    pub challenge: Hash,
    pub last_block_at: i64,
}

state!(AccountType, Block);
