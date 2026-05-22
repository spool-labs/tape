use tape_solana::*;
use tape_core::system::{EpochState, NodePreferences};
use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
use tape_crypto::Hash;

use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// Epoch number this account belongs to.
    pub id: EpochNumber,

    /// Solana slot at which this epoch began.
    pub start_slot: SlotNumber,

    /// Wall-clock timestamp (seconds) when this epoch began.
    pub start_time: i64,

    /// The state of the current epoch.
    pub state: EpochState,

    /// Seed captured from the slot hash when this epoch was created.
    pub nonce: Hash,

    /// Voted on snapshot hash for the previous epoch.
    pub snapshot_hash: Hash,

    /// Voted on assignment hash for the next epoch.
    pub assignment_hash: Hash,

    /// Number of spool groups created in this epoch so far.
    pub total_groups: u64,

    /// Sum of spool storage assigned to nodes at the end of the last epoch.
    pub total_assigned: StorageUnits,

    /// Protocol preferences committed while transitioning into this epoch.
    pub preferences: NodePreferences,
}

tape_solana::state!(AccountType, Epoch);

impl Epoch {
    pub fn has_snapshot_hash(&self) -> bool {
        self.snapshot_hash != Hash::zeroed()
    }

    pub fn has_assignment_hash(&self) -> bool {
        self.assignment_hash != Hash::zeroed()
    }
}
