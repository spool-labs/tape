use steel::*;
use super::AccountType;
use crate::{state, types::EpochIndex};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    pub number: EpochIndex,
    pub last_epoch_at: i64,
}

state!(AccountType, Epoch);
