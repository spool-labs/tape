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
pub struct StakingPool {
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
    pub pool: Pubkey,

    /// The state of this stake.
    pub state: u64,

    /// The amount that may be unstaked.
    pub amount: Coin<TAPE>,

    /// The epoch when this stake was activated.
    pub activated_epoch: EpochNumber,

    /// The epoch unstaking can be initiated (0 if not unstaking).
    pub unstake_epoch: EpochNumber,
}

state!(AccountType, StakingPool);
state!(AccountType, StakedTape);
