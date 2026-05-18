use bytemuck::{Pod, Zeroable};
use num_enum::TryFromPrimitive;
use tape_core::bls::BlsPubkey;
use tape_core::spooler::GroupIndex;
use tape_core::system::NodePreferences;
use tape_core::types::{EpochNumber, NodeId, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;

/// Discriminator for event types.
/// Events are grouped in 0x10-sized ranges for extensibility.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum EventType {
    Unknown = 0,

    // Track
    TrackCertified = 0x13,
    TrackDeleted = 0x14,
    TrackInvalidated = 0x15,
    TrackWritten = 0x16,

    // Tape
    TapeReserved = 0x20,
    TapeDestroyed = 0x21,

    // Node
    NodeRegistered = 0x30,
    NodeJoinedCommittee = 0x31,
    SpoolSynced = 0x32,
    SpoolSettled = 0x33,
    PoolAdvanced = 0x34,

    // Epoch
    EpochCommitted = 0x40,
    EpochAdvanced = 0x41,
    EpochCreated = 0x42,
    CommitteeCreated = 0x43,
    CommitteeResized = 0x44,
    PeerSetResized = 0x45,

    // Staking
    StakeDeposited = 0x50,
    StakeUnlockRequested = 0x51,
    StakeWithdrawn = 0x52,

    // Commission
    CommissionClaimed = 0x60,

    // Vote
    VoteProposed = 0x70,
    VoteRecorded = 0x71,

    // Snapshot
    SnapshotFinalized = 0x72,

    // Assignment
    AssignmentGroupFinalized = 0x80,
}

/// Emitted when a track achieves certification quorum.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackCertified {
    /// Track account address
    pub track: Address,

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
    pub track: Address,

    /// Parent tape address
    pub tape: Address,

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
    pub track: Address,

    /// Invalidation epoch
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, TrackInvalidated);

/// Emitted when a new track write is committed into the tape tree.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackWritten {
    /// The epoch when the track was written
    pub epoch: EpochNumber,

    /// Parent tape address
    pub tape: Address,

    /// Track account address
    pub track: Address,

    /// Track index within the tape
    pub track_number: TrackNumber,

    /// The spool group that is responsible for this track
    pub group: GroupIndex,

    /// The compressed track hash that was added to the tape's merkle tree
    pub track_hash: Hash,
}

tape_solana::event!(EventType, TrackWritten);

/// Emitted when storage capacity is reserved.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TapeReserved {
    /// Tape account address
    pub tape: Address,

    /// Owner who reserved
    pub authority: Address,

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
    pub tape: Address,

    /// Owner who destroyed
    pub authority: Address,
}

tape_solana::event!(EventType, TapeDestroyed);

/// Emitted when a storage node registers.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct NodeRegistered {
    /// Node account address
    pub node: Address,

    /// Assigned unique node ID
    pub id: NodeId,

    /// Node operator pubkey
    pub authority: Address,

    /// Registration epoch
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, NodeRegistered);

/// Emitted when a node joins the next epoch's committee.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct NodeJoinedCommittee {
    /// Node account address
    pub node: Address,

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

/// Emitted when a single spool's owner attests data sync.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SpoolSynced {
    /// Node account address (owner of the synced spool)
    pub node: Address,

    /// Epoch being synced
    pub epoch: EpochNumber,

    /// Spool group containing the spool
    pub group: GroupIndex,

    /// Index within the group (0 .. GROUP_SIZE)
    pub spool: [u8; 8],
}

tape_solana::event!(EventType, SpoolSynced);

/// Emitted when a single spool's pool reward is credited via `SettleSpool`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SpoolSettled {
    /// Node account address (owner of the settled spool)
    pub node: Address,

    /// Previous epoch being settled
    pub epoch: EpochNumber,

    /// Spool group containing the spool
    pub group: GroupIndex,

    /// Index within the group (0 .. GROUP_SIZE)
    pub spool: [u8; 8],
}

tape_solana::event!(EventType, SpoolSettled);

/// Emitted when a pool drains its accumulated `pending_rewards` via `AdvancePool`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct PoolAdvanced {
    /// Node account address (the pool being advanced)
    pub node: Address,

    /// Previous epoch absorbed by this advance
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, PoolAdvanced);

/// Emitted on `commit_epoch` (Active → Closing).
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct EpochCommitted {
    /// Epoch transitioning to Closing
    pub epoch: EpochNumber,

    /// Slot hash captured as the next epoch's nonce
    pub next_nonce: Hash,
}

tape_solana::event!(EventType, EpochCommitted);

/// Emitted when an epoch account is created.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct EpochCreated {
    /// Created epoch account.
    pub epoch: EpochNumber,
}

tape_solana::event!(EventType, EpochCreated);

/// Emitted when an epoch-scoped committee account is created.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CommitteeCreated {
    /// Committee epoch.
    pub epoch: EpochNumber,

    /// Allocated member capacity.
    pub capacity: [u8; 8],
}

tape_solana::event!(EventType, CommitteeCreated);

/// Emitted when an epoch-scoped committee account is resized.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CommitteeResized {
    /// Committee epoch.
    pub epoch: EpochNumber,

    /// Allocated member capacity after this resize. Intermediate resize
    /// transactions may report the previous capacity until the target size is
    /// reached.
    pub capacity: [u8; 8],
}

tape_solana::event!(EventType, CommitteeResized);

/// Emitted when the singleton peer-set account is resized.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct PeerSetResized {
    /// Allocated peer capacity after this resize. Intermediate resize
    /// transactions may report the previous capacity until the target size is
    /// reached.
    pub capacity: [u8; 8],
}

tape_solana::event!(EventType, PeerSetResized);

/// Emitted on `advance_epoch` (Closing → next Syncing).
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct EpochAdvanced {
    /// Previous epoch
    pub old_epoch: EpochNumber,

    /// New epoch
    pub new_epoch: EpochNumber,

    /// Unix timestamp
    pub timestamp: [u8; 8],

    /// Total staked TAPE across the new epoch's active committee
    pub total_stake: [u8; 8],

    /// Active committee size — count of members in `Committee(new_epoch)`.
    /// Distinct from `preferences.committee_size`, which is the cap voted in
    /// by node preferences and applied to future committees.
    pub committee_count: [u8; 8],

    /// Network-level preferences aggregated from this epoch's committee.
    pub preferences: NodePreferences,

    /// Randomness seed for the new epoch (captured at the previous commit)
    pub nonce: Hash,
}

tape_solana::event!(EventType, EpochAdvanced);

/// Emitted when a user stakes TAPE.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeDeposited {
    /// Stake account address
    pub stake: Address,

    /// Staker
    pub authority: Address,

    /// Target pool
    pub pool: Address,

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
    pub stake: Address,

    /// Staker
    pub authority: Address,

    /// Pool
    pub pool: Address,

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
    pub stake: Address,

    /// Staker
    pub authority: Address,

    /// Pool
    pub pool: Address,

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
    pub node: Address,

    /// Node operator
    pub authority: Address,

    /// TAPE flux units claimed
    pub amount: [u8; 8],
}

tape_solana::event!(EventType, CommissionClaimed);

/// Emitted when a snapshot or assignment candidate vote account is created.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteProposed {
    /// `VoteKind` as `u64`.
    pub kind: u64,

    /// Vote account address.
    pub vote: Address,

    /// Epoch whose groups are voting.
    pub voting_epoch: EpochNumber,

    /// Epoch the candidate value applies to.
    pub target_epoch: EpochNumber,

    /// Snapshot or assignment hash.
    pub hash: Hash,

    /// Number of groups required to land this candidate.
    pub total_groups: [u8; 8],
}

tape_solana::event!(EventType, VoteProposed);

/// Emitted when a group records a vote for a snapshot or assignment candidate.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteRecorded {
    /// `VoteKind` as `u64`.
    pub kind: u64,

    /// Vote account address.
    pub vote: Address,

    /// Epoch whose groups are voting.
    pub voting_epoch: EpochNumber,

    /// Epoch the candidate value applies to.
    pub target_epoch: EpochNumber,

    /// Snapshot or assignment hash.
    pub hash: Hash,

    /// The spool group whose owners signed.
    pub group: GroupIndex,

    /// Number of signers in this cert.
    pub signer_count: [u8; 8],

    /// Number of groups recorded after this vote.
    pub signed_groups: [u8; 8],

    /// Number of groups required to land this candidate.
    pub total_groups: [u8; 8],
}

tape_solana::event!(EventType, VoteRecorded);

/// Emitted when the canonical epoch snapshot tape is created.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SnapshotFinalized {
    /// Epoch the snapshot belongs to.
    pub epoch: EpochNumber,

    /// Canonical snapshot hash.
    pub hash: Hash,

    /// Snapshot tape account address.
    pub snapshot_tape: Address,
}

tape_solana::event!(EventType, SnapshotFinalized);

/// Emitted when one group from the canonical assignment is finalized.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AssignmentGroupFinalized {
    /// Epoch the assignment group belongs to.
    pub epoch: EpochNumber,

    /// Canonical assignment hash.
    pub hash: Hash,

    /// Finalized spool group.
    pub group: GroupIndex,

    /// Group account address.
    pub group_account: Address,

    /// Per-spool committed size for this group.
    pub size: StorageUnits,

    /// Number of groups finalized after this group lands.
    pub total_groups: [u8; 8],

    /// Total assigned storage after this group lands.
    pub total_assigned: StorageUnits,
}

tape_solana::event!(EventType, AssignmentGroupFinalized);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_values() {
        assert_eq!(EventType::TrackCertified as u8, 0x13);
        assert_eq!(EventType::TapeReserved as u8, 0x20);
        assert_eq!(EventType::NodeRegistered as u8, 0x30);
        assert_eq!(EventType::EpochCommitted as u8, 0x40);
        assert_eq!(EventType::EpochAdvanced as u8, 0x41);
        assert_eq!(EventType::StakeDeposited as u8, 0x50);
        assert_eq!(EventType::CommissionClaimed as u8, 0x60);
        assert_eq!(EventType::VoteProposed as u8, 0x70);
        assert_eq!(EventType::VoteRecorded as u8, 0x71);
        assert_eq!(EventType::SnapshotFinalized as u8, 0x72);
        assert_eq!(EventType::SpoolSettled as u8, 0x33);
        assert_eq!(EventType::PoolAdvanced as u8, 0x34);
        assert_eq!(EventType::AssignmentGroupFinalized as u8, 0x80);
    }

    #[test]
    fn test_event_sizes() {
        assert!(TrackCertified::size_of() < 1024);
        assert!(TrackDeleted::size_of() < 1024);
        assert!(TapeReserved::size_of() < 1024);
        assert!(EpochCommitted::size_of() < 1024);
        assert!(EpochAdvanced::size_of() < 1024);
        assert!(SpoolSynced::size_of() < 1024);
        assert!(SpoolSettled::size_of() < 1024);
        assert!(PoolAdvanced::size_of() < 1024);
        assert!(StakeDeposited::size_of() < 1024);
        assert!(VoteProposed::size_of() < 1024);
        assert!(VoteRecorded::size_of() < 1024);
        assert!(SnapshotFinalized::size_of() < 1024);
        assert!(AssignmentGroupFinalized::size_of() < 1024);
    }
}
