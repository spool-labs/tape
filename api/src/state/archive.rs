use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;
use crate::program::FUTURE_EPOCHS;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price: Coin<TAPE>,

    /// The storage capacity reserved in future epochs.
    pub capacity_used: FutureUsage<FUTURE_EPOCHS>,

    /// The fees collected in future epochs.
    pub fees_collected: FutureRewards<FUTURE_EPOCHS>,

    /// The fees collected in the last epoch including carry over.
    pub rewards_pool: Coin<TAPE>,

    /// The rewards paid out so far in this epoch.
    pub rewards_paid: Coin<TAPE>,

    /// The capacity reserved in the last epoch.
    pub recent_usage: StorageUnits,
}

state!(AccountType, Archive);
