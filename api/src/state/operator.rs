use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

const EPOCH_HISTORY: usize = 256;
const PENDING_VALUES: usize = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StorageNode {
    /// The unique identifier for this pool.
    pub id: NodeId,

    /// The authority that owns this node.
    pub authority: Pubkey,

    /// The staking pool associated with this node.
    pub pool: StakingPool<EPOCH_HISTORY, PENDING_VALUES>,

    /// Metadata about this storage node.
    pub metadata: NodeMetadata,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct NodeMetadata {
    /// The name of this node storage node.
    pub name: [u8; 32],

    /// The storage capacity of the node in bytes.
    pub storage_capacity: u64,

    /// The storage used by the node in bytes.
    pub storage_used: u64,

    /// The SocketAddr of the node
    pub network_address: NetworkAddress,

    /// The public key used for TLS connections to this node.
    pub network_tls: Pubkey,
}

state!(AccountType, StorageNode);
