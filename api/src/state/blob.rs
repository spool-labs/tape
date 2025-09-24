use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum BlobState {
    Unknown = 0,
    Registered,
    Certified,
    Invalidated,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Blob {
    /// The authority that owns this blob.
    pub authority: Pubkey,

    /// The size of the blob data in bytes.
    pub size: StorageUnits,

    /// The state of this blob.
    pub state: u64,

    /// The epoch when this blob was registered.
    pub registered_epoch: EpochNumber,

    /// The epoch when this blob was certified.
    pub certified_epoch: EpochNumber,

    /// The tape this blob is stored on.
    pub tape_resource: Pubkey,

    /// The hash of the blob data.
    pub hash: Hash,
}

state!(AccountType, Blob);
