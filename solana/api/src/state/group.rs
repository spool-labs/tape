use tape_solana::*;
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::Spool;
use tape_core::types::{EpochNumber, SpoolBitmap, StorageUnits};
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Group {
    /// Group index within the epoch.
    pub id: GroupIndex,

    /// Epoch this group belongs to.
    pub epoch: EpochNumber,

    /// Spool size at the time of assignment, all spools in a group have the same size.
    pub size: StorageUnits,

    /// Bitmap of spools that are done syncing.
    pub synced: SpoolBitmap,

    /// Owners and BLS keys for this group's spools. A network with n spools
    /// per group fills the first n and leaves the rest zeroed.
    pub spools: [Spool; MAX_GROUP_SIZE],
}

unsafe impl Zeroable for Group {}
unsafe impl Pod for Group {}

tape_solana::state!(AccountType, Group);
