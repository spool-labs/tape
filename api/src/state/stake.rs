use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Stake {
    /// The authority that owns this stake.
    pub authority: Pubkey,

    /// The pool this stake is associated with.
    pub pool: Pubkey,

    /// The staking details (amount, activation, state, etc).
    pub inner: StakedTape,
}

tape_solana::state!(AccountType, Stake);
