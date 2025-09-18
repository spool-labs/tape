use steel::*;
use super::AccountType;
use crate::{state, types::EpochNumber};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,
}

state!(AccountType, Epoch);

pub fn current_epoch(epoch: &Epoch) -> EpochNumber {
    epoch.id
}

pub fn next_epoch(epoch: &Epoch) -> EpochNumber {
    EpochNumber::new(epoch.id.as_u64() + 1)
}
