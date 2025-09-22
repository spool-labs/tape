use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {
    /// The total amount of stake in the treasury.
    pub total_stake: Coin<TAPE>,
}

state!(AccountType, Treasury);
