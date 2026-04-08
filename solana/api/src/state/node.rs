use tape_crypto::address::Address;
use tape_solana::*;
use tape_core::staking::StakingPool;
use tape_core::system::{Blacklist, NodeMetadata, NodePreferences};
use tape_core::types::EpochNumber;
use tape_core::types::NodeId;

use super::AccountType;
use crate::program::{
    BLACKLIST_SIZE,
    EPOCH_VALUES,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Node {
    /// The unique identifier for this pool.
    pub id: NodeId,

    /// The authority that owns this node.
    pub authority: Address,

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

    /// The last epoch this node synced via SyncEpoch.
    /// Prevents double-sync in the same epoch.
    pub latest_sync_epoch: EpochNumber,

    /// The last epoch this node's pool was advanced via AdvancePool.
    /// Prevents double-advance in the same epoch.
    /// Separate from latest_sync_epoch because a node in both committees
    /// needs to both sync AND advance.
    pub latest_advance_epoch: EpochNumber,
}


tape_solana::state!(AccountType, Node);
