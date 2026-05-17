use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use tape_crypto::address::Address;

use crate::types::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Member {
    pub node: Address,
    pub stake: Coin<TAPE>,
    pub blacklist: StorageUnits,
    pub spools: u64,
}

impl Member {
    pub fn new(node: Address, stake: Coin<TAPE>) -> Self {
        Member {
            node,
            stake,
            ..Member::zeroed()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitteeError {
    AlreadyPresent { idx: usize },
    Full,
    NotFull,
    NotFound,
    NotBetter { min_idx: usize, min_stake: Coin<TAPE> },
    ZeroStake,
}
