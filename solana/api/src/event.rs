use bytemuck::{Pod, Zeroable};
use num_enum::TryFromPrimitive;
use solana_program::pubkey::Pubkey;
use tape_core::bls::BlsPubkey;
use tape_core::spooler::SpoolGroup;
use tape_core::system::NodePreferences;
use tape_core::types::{EpochNumber, NodeId, StorageUnits, TrackNumber};
use tape_crypto::Hash;

/// Discriminator for event types.
/// Events are grouped in 0x10-sized ranges for extensibility.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum EventType {
    Unknown = 0,

    // Track events (0x10 range)
    SnapshotEpochInitialized = 0x10,
    SnapshotGroupCertified = 0x11,
    SnapshotEpochFinalized = 0x12,
    TrackCertified = 0x13,
    TrackDeleted = 0x14,
    TrackInvalidated = 0x15,
    TrackWritten = 0x16,

    // Tape events (0x20 range)
    TapeReserved = 0x20,
    TapeDestroyed = 0x21,

    // Node events (0x30 range)
    NodeRegistered = 0x30,
    NodeJoinedCommittee = 0x31,
    NodeSynced = 0x32,
    PoolAdvanced = 0x33,

    // Epoch events (0x40 range)
    EpochAdvanced = 0x40,

    // Staking events (0x50 range)
    StakeDeposited = 0x50,
    StakeUnlockRequested = 0x51,
    StakeWithdrawn = 0x52,

    // Commission events (0x60 range)
    CommissionClaimed = 0x60,
}

/// Emitted when an epoch's snapshot manifest and tape are initialized.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SnapshotEpochInitialized {
    pub epoch: EpochNumber,
    pub parent_epoch: EpochNumber,
    pub tape: Pubkey,
}

tape_solana::event!(EventType, SnapshotEpochInitialized);

/// Emitted when a canonical snapshot group is sealed into the manifest/tape.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SnapshotGroupCertified {
    pub epoch: EpochNumber,
    pub group: SpoolGroup,
    pub tape: Pubkey,
    pub track: Pubkey,
    pub track_number: TrackNumber,
    pub commitment: Hash,
    pub signer_count: [u8; 8],
    pub signer_weight: [u8; 8],
}

tape_solana::event!(EventType, SnapshotGroupCertified);

/// Emitted when a snapshot epoch becomes the canonical tail.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SnapshotEpochFinalized {
    pub epoch: EpochNumber,
    pub parent_epoch: EpochNumber,
    pub tail_epoch: EpochNumber,
}

tape_solana::event!(EventType, SnapshotEpochFinalized);

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

/// Emitted when a new track write is committed into the tape tree.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackWritten {
    pub epoch: EpochNumber,
    pub track: Pubkey,
    pub tape: Pubkey,
    pub track_number: TrackNumber,
    pub spool_group: [u8; 8],
    pub track_hash: Hash,
}

tape_solana::event!(EventType, TrackWritten);

/// Emitted when storage capacity is reserved.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TapeReserved {
    /// Tape account address
    pub tape: Pubkey,
    /// Owner who reserved
    pub authority: Pubkey,
    /// Reserved capacity in bytes
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
    /// Current BLS public key used once this node rotates into the active committee
    pub key: BlsPubkey,
    /// Total blacklisted storage units carried into committee scoring/rewards
    pub blacklist: StorageUnits,
    /// Storage preferences used when the joined committee rotates into active service
    pub preferences: NodePreferences,
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
    /// Epoch phase after this sync (Syncing, Settling, or Active)
    pub phase: u64,
}

tape_solana::event!(EventType, NodeSynced);

/// Emitted when a node advances its staking pool.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct PoolAdvanced {
    /// Node account address
    pub node: Pubkey,
    /// Node ID
    pub id: NodeId,
    /// Current epoch
    pub epoch: EpochNumber,
    /// Epoch phase after this advance (Settling or Active)
    pub phase: u64,
}

tape_solana::event!(EventType, PoolAdvanced);

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
    /// Randomness seed for leader schedule
    pub nonce: Hash,
    /// Epoch phase after advance (always Syncing)
    pub phase: u64,
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
        assert_eq!(EventType::SnapshotEpochInitialized as u8, 0x10);
        assert_eq!(EventType::SnapshotGroupCertified as u8, 0x11);
        assert_eq!(EventType::SnapshotEpochFinalized as u8, 0x12);
        assert_eq!(EventType::TrackCertified as u8, 0x13);
        assert_eq!(EventType::TapeReserved as u8, 0x20);
        assert_eq!(EventType::NodeRegistered as u8, 0x30);
        assert_eq!(EventType::EpochAdvanced as u8, 0x40);
        assert_eq!(EventType::StakeDeposited as u8, 0x50);
        assert_eq!(EventType::CommissionClaimed as u8, 0x60);
    }

    #[test]
    fn test_event_sizes() {
        // Verify events fit within Solana's 1024-byte log limit
        assert!(SnapshotEpochInitialized::size_of() < 1024);
        assert!(SnapshotGroupCertified::size_of() < 1024);
        assert!(SnapshotEpochFinalized::size_of() < 1024);
        assert!(TrackCertified::size_of() < 1024);
        assert!(TrackDeleted::size_of() < 1024);
        assert!(TapeReserved::size_of() < 1024);
        assert!(EpochAdvanced::size_of() < 1024);
        assert!(NodeSynced::size_of() < 1024);
        assert!(PoolAdvanced::size_of() < 1024);
        assert!(StakeDeposited::size_of() < 1024);
    }

    #[test]
    fn snapshot_group_certified_layout_preserves_domain_types() {
        let event = SnapshotGroupCertified {
            epoch: EpochNumber(7),
            group: SpoolGroup(3),
            tape: Pubkey::new_unique(),
            track: Pubkey::new_unique(),
            track_number: TrackNumber(11),
            commitment: Hash::from([0x33; 32]),
            signer_count: 19u64.to_le_bytes(),
            signer_weight: 42u64.to_le_bytes(),
        };

        let bytes = event.to_bytes();
        let recovered = SnapshotGroupCertified::try_from_bytes(&bytes).expect("parse event");

        assert_eq!(recovered.group, SpoolGroup(3));
        assert_eq!(recovered.track_number, TrackNumber(11));
    }
}
