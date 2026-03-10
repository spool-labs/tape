use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::program::EPOCH_HISTORY;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct History {
    /// The node this history is associated with.
    pub node: Pubkey,

    /// The epoch when this history was registered.
    pub registered_epoch: EpochNumber,

    /// The last epoch this history was updated.
    pub latest_epoch: EpochNumber,

    /// The staking pool history of this node.
    pub inner: PoolHistory<EPOCH_HISTORY>,
}


tape_solana::state!(AccountType, History);
