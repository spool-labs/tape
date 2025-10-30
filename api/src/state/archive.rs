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

    /// The capacity reserved in the last epoch.
    pub recent_reserved: StorageUnits,

    /// The data stored in the last epoch.
    pub recent_stored: StorageUnits,

    /// The fees collected in the last epoch.
    pub recent_fees: Coin<TAPE>,
}

state!(AccountType, Archive);
