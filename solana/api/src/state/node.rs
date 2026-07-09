use tape_crypto::address::Address;
use tape_solana::*;
use tape_core::staking::{RateSpan, StakingPool};
use tape_core::system::{NodeMetadata, NodePreferences};
use tape_core::types::EpochNumber;
use tape_core::types::NodeId;

use super::AccountType;
use crate::program::EPOCH_VALUES;

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

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,

    /// The last epoch this node synced via SyncSpool.
    pub latest_sync_epoch: EpochNumber,

    /// The last epoch this node's pool was advanced via AdvancePool.
    pub latest_advance_epoch: EpochNumber,

    /// First epoch covered by the current open pool rate span.
    pub rate_span_start: EpochNumber,
}

impl Node {
    pub fn rate_span(&self, address: Address, current_epoch: EpochNumber) -> RateSpan {
        RateSpan {
            node: address,
            start_epoch: self.rate_span_start,
            end_epoch: current_epoch,
            rate: self.pool.get_current_rate(),
        }
    }
}

tape_solana::state!(AccountType, Node);
