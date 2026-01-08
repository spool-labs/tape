use bytemuck::{Pod, Zeroable};
use num_enum::TryFromPrimitive;
use solana_program::pubkey::Pubkey;
use tape_core::types::{EpochNumber, NodeId, StorageUnits};
use tape_core::prelude::Hash;

/// Discriminator for event types.
/// Events are grouped in 0x10-sized ranges for extensibility.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum EventType {
    Unknown = 0,

    // Track events (0x10 range)
    TrackRegistered = 0x10,
    TrackCertified = 0x11,
    TrackDeleted = 0x12,
    TrackInvalidated = 0x13,

    // Tape events (0x20 range)
    TapeReserved = 0x20,
    TapeDestroyed = 0x21,

    // Node events (0x30 range)
    NodeRegistered = 0x30,
    NodeJoinedCommittee = 0x31,
    NodeSynced = 0x32,

    // Epoch events (0x40 range)
    EpochAdvanced = 0x40,

    // Staking events (0x50 range)
    StakeDeposited = 0x50,
    StakeUnlockRequested = 0x51,
    StakeWithdrawn = 0x52,

    // Commission events (0x60 range)
    CommissionClaimed = 0x60,
}

/// Emitted when a new track is registered on-chain.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackRegistered {
    /// Track account address
    pub track: Pubkey,
    /// Parent tape address
    pub tape: Pubkey,
    /// User-defined identifier hash
    pub key: Hash,
    /// Total storage in MB
    pub size: StorageUnits,
    /// Erasure coding Merkle root
    pub commitment: Hash,
    /// Registration epoch
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, TrackRegistered);

/// Emitted when a track achieves certification quorum.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackCertified {
    /// Track account address
    pub track: Pubkey,
    /// Certification epoch
    pub epoch: EpochNumber,
    /// Committee members who signed
    pub signer_count: [u8; 8],
    /// Total spool weight of signers
    pub signer_weight: [u8; 8],
}

tape_solana::event!(EventType, TrackCertified);

/// Emitted when a track is hard-deleted (account closed).
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackDeleted {
    /// Track account address
    pub track: Pubkey,
    /// Parent tape address
    pub tape: Pubkey,
    /// Track key for reference
    pub key: Hash,
    /// Storage being freed
    pub size: StorageUnits,
}

tape_solana::event!(EventType, TrackDeleted);

/// Emitted when a track is soft-deleted (marked invalid, account preserved).
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackInvalidated {
    /// Track account address
    pub track: Pubkey,
    /// Invalidation epoch
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, TrackInvalidated);

/// Emitted when storage capacity is reserved.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TapeReserved {
    /// Tape account address
    pub tape: Pubkey,
    /// Owner who reserved
    pub authority: Pubkey,
    /// Reserved capacity in MB
    pub capacity: StorageUnits,
    /// First active epoch
    pub active_epoch: EpochNumber,
    /// Expiration epoch
    pub expiry_epoch: EpochNumber,
    /// TAPE flux units paid
    pub cost: [u8; 8],
}

tape_solana::event!(EventType, TapeReserved);

/// Emitted when a tape is closed.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TapeDestroyed {
    /// Tape account address
    pub tape: Pubkey,
    /// Owner who destroyed
    pub authority: Pubkey,
}

tape_solana::event!(EventType, TapeDestroyed);

/// Emitted when a storage node registers.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct NodeRegistered {
    /// Node account address
    pub node: Pubkey,
    /// Assigned unique node ID
    pub id: NodeId,
    /// Node operator pubkey
    pub authority: Pubkey,
    /// Registration epoch
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, NodeRegistered);

/// Emitted when a node joins the active committee.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct NodeJoinedCommittee {
    /// Node account address
    pub node: Pubkey,
    /// Node ID
    pub id: NodeId,
    /// Stake in TAPE flux units
    pub stake: [u8; 8],
    /// When node becomes active
    pub activation_epoch: EpochNumber,
}

tape_solana::event!(EventType, NodeJoinedCommittee);

/// Emitted when a node completes epoch sync.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct NodeSynced {
    /// Node account address
    pub node: Pubkey,
    /// Node ID
    pub id: NodeId,
    /// Synced epoch
    pub epoch: EpochNumber,
    /// Hash of spool assignments
    pub spools_hash: Hash,
}

tape_solana::event!(EventType, NodeSynced);

/// Emitted when the protocol epoch advances.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct EpochAdvanced {
    /// Previous epoch
    pub old_epoch: EpochNumber,
    /// New epoch
    pub new_epoch: EpochNumber,
    /// Unix timestamp
    pub timestamp: [u8; 8],
    /// Active committee count
    pub committee_size: [u8; 8],
    /// Total staked TAPE
    pub total_stake: [u8; 8],
    /// Current price per StorageUnit
    pub storage_price: [u8; 8],
    /// Total network capacity
    pub storage_capacity: StorageUnits,
}

tape_solana::event!(EventType, EpochAdvanced);

/// Emitted when a user stakes TAPE.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeDeposited {
    /// Stake account address
    pub stake: Pubkey,
    /// Staker
    pub authority: Pubkey,
    /// Target pool
    pub pool: Pubkey,
    /// TAPE flux units
    pub amount: [u8; 8],
    /// When stake activates
    pub activation_epoch: EpochNumber,
}

tape_solana::event!(EventType, StakeDeposited);

/// Emitted when unstake is initiated (starts cooldown).
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeUnlockRequested {
    /// Stake account address
    pub stake: Pubkey,
    /// Staker
    pub authority: Pubkey,
    /// Pool
    pub pool: Pubkey,
    /// Amount being unlocked
    pub amount: [u8; 8],
    /// When withdrawal available
    pub withdraw_epoch: EpochNumber,
}

tape_solana::event!(EventType, StakeUnlockRequested);

/// Emitted when stake is fully withdrawn.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeWithdrawn {
    /// Stake account address
    pub stake: Pubkey,
    /// Staker
    pub authority: Pubkey,
    /// Pool
    pub pool: Pubkey,
    /// Principal returned
    pub principal: [u8; 8],
    /// Rewards earned
    pub rewards: [u8; 8],
}

tape_solana::event!(EventType, StakeWithdrawn);

/// Emitted when a node operator claims commission.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CommissionClaimed {
    /// Node account address
    pub node: Pubkey,
    /// Node operator
    pub authority: Pubkey,
    /// TAPE flux units claimed
    pub amount: [u8; 8],
}

tape_solana::event!(EventType, CommissionClaimed);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_values() {
        assert_eq!(EventType::TrackRegistered as u8, 0x10);
        assert_eq!(EventType::TrackCertified as u8, 0x11);
        assert_eq!(EventType::TapeReserved as u8, 0x20);
        assert_eq!(EventType::NodeRegistered as u8, 0x30);
        assert_eq!(EventType::EpochAdvanced as u8, 0x40);
        assert_eq!(EventType::StakeDeposited as u8, 0x50);
        assert_eq!(EventType::CommissionClaimed as u8, 0x60);
    }

    #[test]
    fn test_event_sizes() {
        // Verify events fit within Solana's 1024-byte log limit
        assert!(TrackRegistered::size_of() < 1024);
        assert!(TrackCertified::size_of() < 1024);
        assert!(TrackDeleted::size_of() < 1024);
        assert!(TapeReserved::size_of() < 1024);
        assert!(EpochAdvanced::size_of() < 1024);
        assert!(StakeDeposited::size_of() < 1024);
    }
}
