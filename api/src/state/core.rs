use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The number of pools currently registered.
    pub total_pools: u64,
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
    /// The unique identifier for this archive.
    pub id: ArchiveNumber,

    /// The encoding scheme used by this archive (e.g., erasure coding = 0, replication = 1).
    pub encoding: u64,

    /// The number of data shards (spools) in the encoding scheme.
    pub spool_count: u64,

    /// The total storage capacity of the archive.
    pub storage_capacity: u64,

    /// The total storage used by the archive.
    pub storage_used: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {
    /// The total amount of stake in the treasury.
    pub total_stake: Coin<TAPE>,
}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Archive);
state!(AccountType, Treasury);
