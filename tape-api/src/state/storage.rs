use steel::*;
use super::AccountType;
use crate::{
    state, 
    types::EpochIndex, 
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Storage {
    pub authority: Pubkey,
    pub size: u64,
    pub start_epoch: EpochIndex,
    pub end_epoch: EpochIndex,
}

state!(AccountType, Storage);
