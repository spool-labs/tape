use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakedTape {
    /// The authority that owns this stake.
    pub authority: Pubkey,

    /// The pool this stake is associated with.
    pub node: Pubkey,

    /// The staking details (amount, activation, state, etc).
    pub inner: Stake,
}

state!(AccountType, StakedTape);


