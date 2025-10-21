use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The timestamp (in milliseconds) of the last epoch.
    pub last_epoch_ms: i64,

    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price: Coin<TAPE>,
}

state!(AccountType, Epoch);
