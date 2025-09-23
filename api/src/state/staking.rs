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

state!(AccountType, StorageNode);
state!(AccountType, StakedTape);
