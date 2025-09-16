use steel::*;
use super::AccountType;
use crate::{
    state, 
    types::EpochIndex, 
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Blob {
    pub authority: Pubkey,
    pub storage: Pubkey,

    pub size: u64,
    pub certified_epoch: EpochIndex,
}

state!(AccountType, Blob);
