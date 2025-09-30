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

    /// The amount that may be unstaked.
    pub amount: Coin<TAPE>,

    /// The epoch when this stake was activated.
    pub activated_epoch: EpochNumber,

    /// The state of this stake.
    pub state: StakeState,
}

state!(AccountType, StakedTape);


