use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;
use crate::program::{
    EPOCH_VALUES,
    EPOCH_HISTORY
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Node {
    /// The unique identifier for this pool.
    pub id: NodeId,

    /// The authority that owns this node.
    pub authority: Pubkey,

    /// The staking pool associated with this node.
    pub pool: StakingPool<EPOCH_VALUES>,

    /// The staking pool associated with this node.
    pub history: PoolHistory<EPOCH_HISTORY>,

    /// Metadata about this storage node.
    pub metadata: NodeMetadata,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,
}


state!(AccountType, Node);
