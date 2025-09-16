use steel::*;
use super::AccountType;
use crate::{
    state, 
    types::{ 
        NodeID, 
        EpochIndex, 
        BlockIndex, 
        NetworkAddress
    },
    NAME_LENGTH
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Member {
    pub node_id: NodeID,

    pub authority: Pubkey,
    pub name: [u8; NAME_LENGTH],
    pub network_address: NetworkAddress,

    pub stake: u64,
    pub commission_rate: u64,
    pub write_price: u64,
    pub storage_price: u64,
    pub storage_capacity: u64,

    pub last_active_epoch: EpochIndex,
    pub last_block_index: BlockIndex,
}

state!(AccountType, Member);
