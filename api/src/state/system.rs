use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The number of storage nodes currently registered.
    pub total_nodes: u64,

    /// The total amount of stake in the treasury.
    pub total_staked: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: u64,

    /// The total storage used by the archive.
    pub storage_used: u64,
}


#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Feature {
}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Archive);
state!(AccountType, Feature);
