use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Blob {
    /// The authority that owns this blob.
    pub authority: Pubkey,

    /// The size of the blob data in bytes.
    pub size: u64,

    /// The epoch when this blob was registered.
    pub registered_epoch: EpochNumber,

    /// The epoch when this blob was certified.
    pub certified_epoch: EpochNumber,

    /// The tape this blob is stored on.
    pub tape: Pubkey,

    /// The hash of the blob data.
    pub hash: Hash,
}

state!(AccountType, Blob);
