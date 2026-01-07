use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The unique identifier for this tape.
    pub id: TapeNumber,

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

    /// The count of tracks on this tape.
    pub track_count: u64,
}

tape_solana::state!(AccountType, Tape);
