use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The authority that owns this tape.
    pub authority: Pubkey,

    /// The amount of storage reserved.
    pub capacity: StorageUnits,

    /// The amount of storage used.
    pub used: StorageUnits,

    /// The epoch when this resource is active.
    pub active_epoch: EpochNumber,

    /// The epoch when this resource expires.
    pub expiry_epoch: EpochNumber,

    /// The slot of the first track on this tape.
    pub first_track: SlotNumber,

    /// The slot of the last track of this tape.
    pub last_track: SlotNumber,

    /// The count of tracks on this tape.
    pub total_tracks: u64,
}

state!(AccountType, Tape);
