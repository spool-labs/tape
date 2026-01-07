use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::program::{
    EPOCH_VALUES,
    BLACKLIST_SIZE,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Node {
    /// The unique identifier for this pool.
    pub id: NodeId,

    /// The authority that owns this node.
    pub authority: Pubkey,

    /// Metadata about this storage node.
    pub metadata: NodeMetadata,

    /// Preferences for this storage node.
    pub preferences: NodePreferences,

    /// The staking pool associated with this node.
    pub pool: StakingPool<EPOCH_VALUES>,

    /// Blacklist for this node.
    pub blacklist: Blacklist<BLACKLIST_SIZE>,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,

    /// The last epoch this node was updated.
    pub latest_epoch: EpochNumber,
}


tape_solana::state!(AccountType, Node);
