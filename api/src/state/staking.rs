use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum StakeState {
    Unknown = 0,
    Active,
    Unstaking 
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakedTape {
    /// The authority that owns this stake.
    pub authority: Pubkey,

    /// The pool this stake is associated with.
    pub node: Pubkey,

    /// The state of this stake.
    pub state: u64,

    /// The amount that may be unstaked.
    pub amount: Coin<TAPE>,

    /// The epoch when this stake was activated.
    pub activated_epoch: EpochNumber,

    /// The epoch unstaking can be initiated (0 if not unstaking).
    pub unstake_epoch: EpochNumber,
}

state!(AccountType, StakedTape);
