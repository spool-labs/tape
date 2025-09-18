use steel::*;
use super::AccountType;
use crate::{state, types::*,};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Pool {
    /// The unique identifier for this pool.
    pub id: PoolNumber,

    /// The authority that owns this node.
    pub authority: Pubkey,

    /// The total stake balance in the pool.
    pub total_stake: Coin<TAPE>,

    /// The commission rate taken by the pool (in basis points).
    pub commission_rate: BasisPoints,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,

    /// The storage nodes associated with this pool.
    pub storage_node: StorageNode,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StorageNode {
    /// The name of this node storage node.
    pub name: [u8; 32],

    /// The storage capacity of the node in bytes.
    pub storage_capacity: u64,

    /// The storage used by the node in bytes.
    pub storage_used: u64,

    /// The version of software the node is running.
    pub version: VersionNumber,

    /// The SocketAddr of the node
    pub network_address: NetworkAddress,

    /// The public key used for TLS connections to this node.
    pub network_tls: Pubkey,
}


state!(AccountType, Pool);
