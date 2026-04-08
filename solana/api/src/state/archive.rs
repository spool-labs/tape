use tape_solana::*;
use tape_core::system::EpochSchedule;
use tape_core::types::StorageUnits;
use tape_core::types::coin::{Coin, TAPE};
use super::AccountType;
use crate::program::FUTURE_EPOCHS;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price: Coin<TAPE>,

    /// The archive schedule for future epochs.
    pub schedule: EpochSchedule<FUTURE_EPOCHS>,

    /// The capacity reserved in the last epoch.
    pub recent_usage: StorageUnits,

    /// The fees collected in the last epoch including carry over.
    pub rewards_pool: Coin<TAPE>,

    /// The rewards paid out so far in this epoch.
    pub rewards_paid: Coin<TAPE>,

    /// The total number of tapes in the archive.
    pub tape_count: u64,
}

tape_solana::state!(AccountType, Archive);
