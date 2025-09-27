use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StorageNode {
    /// The unique identifier for this pool.
    pub id: NodeId,

    /// The authority that owns this node.
    pub authority: Pubkey,

    /// The staking pool associated with this node.
    pub pool: StakingPool,

    /// Metadata about this storage node.
    pub metadata: NodeMetadata,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakingPool {
    /// The total stake balance in the pool.
    pub total_staked: Coin<TAPE>,

    /// The commission rate taken by the pool (in basis points).
    pub commission_rate: BasisPoints,
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
