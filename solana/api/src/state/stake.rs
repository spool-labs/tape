use tape_crypto::address::Address;
use tape_solana::*;
use tape_core::prelude::*;

use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Stake {
    /// The authority that owns this stake.
    pub authority: Address,

    /// The pool this stake is associated with.
    pub pool: Address,

    /// The staking details (amount, activation, state, etc).
    pub inner: StakedTape,
}

tape_solana::state!(AccountType, Stake);
