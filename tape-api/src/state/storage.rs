use steel::*;
use super::AccountType;
use crate::{
    state, 
    types::EpochNumber, 
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Storage {
    pub authority: Pubkey,
    pub size: u64,
    pub start_epoch: EpochNumber,
    pub end_epoch: EpochNumber,
}

state!(AccountType, Storage);
