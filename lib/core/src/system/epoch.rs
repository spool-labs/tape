use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, IntoPrimitive, TryFromPrimitive)]
pub enum EpochPhase {
    Unknown = 0,

    /// The epoch is live but still transferring spool data from the previous
    /// epoch's assignments.
    Sync,

    /// The new committee is producing the metadata snapshot of the
    /// previous epoch.
    Snapshot,

    /// The long-running active phase: nodes serve their spool assignments.
    Active,

    /// EPOCH_DURATION has elapsed; the next epoch's seed/nonce has been
    /// captured and the next-epoch group accounts are being populated.
    Closing,

    /// This epoch is completed, it is no longer live.
    Completed,
}

/// Per-epoch state machine. Holds the current phase plus group-completion
/// counters that drive passive phase transitions.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct EpochState {
    /// Current `EpochPhase` (stored as `u64` for Pod-safety).
    pub phase: u64,

    /// Number of spool groups that have reached the sync threshold.
    pub synced_count: u64,
}

impl EpochState {
    #[inline]
    pub fn phase(&self) -> Option<EpochPhase> {
        EpochPhase::try_from(self.phase).ok()
    }
}
