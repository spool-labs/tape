use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

pub const FUTURE_EPOCHS: usize = 256;

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
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price_per_unit: Coin<TAPE>,

    /// The price per unit for writing data in TAPE.
    pub write_price_per_unit: Coin<TAPE>,

    /// The total storage used per epoch.
    pub storage_used: RingBuffer<StorageUnits, FUTURE_EPOCHS>,

    /// The collected fees per epoch.
    pub fees_collected: RingBuffer<Coin<TAPE>, FUTURE_EPOCHS>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Feature {}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Archive);
state!(AccountType, Treasury);
state!(AccountType, Feature);
