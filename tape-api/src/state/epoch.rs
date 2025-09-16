use steel::*;
use super::AccountType;
use crate::{state, types::EpochNumber};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    pub number: EpochNumber,
    pub last_epoch_at: i64,
}

state!(AccountType, Epoch);
