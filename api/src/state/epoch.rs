use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The timestamp (in seconds) of the last epoch.
    pub last_epoch: i64,
}

tape_solana::state!(AccountType, Epoch);
