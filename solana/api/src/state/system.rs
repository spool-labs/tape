use tape_solana::*;
use tape_core::types::{EpochNumber, VersionId};
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// Current epoch number.
    pub current_epoch: EpochNumber,

    /// Minimum protocol version.
    pub min_version: VersionId,

    /// Total registered storage nodes.
    pub total_nodes: u64,

    /// Member capacity of newly created committees.
    pub committee_size: u64,

    /// Target number of spool groups for future epoch creation.
    pub target_group_count: u64,

    /// Number of spool groups live in the current epoch.
    pub live_group_count: u64,
}

tape_solana::state!(AccountType, System);
