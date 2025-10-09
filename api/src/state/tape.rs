use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The authority that owns this resource.
    pub authority: Pubkey,

    /// The amount of storage reserved.
    pub capacity: StorageUnits,

    /// The amount of storage used.
    pub used: StorageUnits,

    /// The epoch when this resource is active.
    pub active_epoch: EpochNumber,

    /// The epoch when this resource expires.
    pub expiry_epoch: EpochNumber,

    /// The count of blobs stored in this resource.
    pub total_blobs: u64,
}

state!(AccountType, Tape);
