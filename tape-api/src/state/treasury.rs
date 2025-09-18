use steel::*;
use super::AccountType;
use crate::{state, types::{Coin, TAPE}};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {
    /// The total amount of stake in the treasury.
    pub total_stake: Coin<TAPE>,
}

state!(AccountType, Treasury);
