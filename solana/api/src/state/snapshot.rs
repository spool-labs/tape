use tape_core::prelude::*;
use tape_core::types::GroupBitmap;
use tape_solana::*;

use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Snapshot {
    /// The epoch for which this snapshot belongs to
    pub epoch: EpochNumber,

    /// The state of this snapshot (Registered = 0, PartiallyCertified = 1, Finalized = 2)
    pub state: u64,

    /// A bitmap of SpoolGroups that have contributed to this snapshot. Each bit corresponds to a SpoolGroup index
    pub group_bitmap: GroupBitmap,
}

tape_solana::state!(AccountType, Snapshot);
