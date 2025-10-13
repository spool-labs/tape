use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price: Coin<TAPE>,

    /// The total storage per epoch.
    pub future_usage: FutureUsage<FUTURE_EPOCHS>,

    /// The total rewards per epoch.
    pub future_rewards: FutureRewards<FUTURE_EPOCHS>,
}

state!(AccountType, Archive);
