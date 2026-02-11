use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct SnapshotState {
    /// Address of the most recently registered snapshot track.
    pub head: Pubkey,

    /// Commitment hash of the head track.
    pub commitment: Hash,

    /// Total number of snapshot tracks registered.
    pub count: u64,

    /// Last fully certified epoch (all SPOOL_GROUP_COUNT chunks certified).
    pub latest_epoch: EpochNumber,

    /// Epoch currently being certified (may be partially done).
    pub certifying_epoch: EpochNumber,

    /// Number of chunks certified for `certifying_epoch` so far.
    pub certified_count: u64,

    /// Cumulative snapshot data size.
    pub total_size: StorageUnits,
}

tape_solana::state!(AccountType, SnapshotState);
