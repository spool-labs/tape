use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Track {
    /// The tape this track is stored on.
    pub tape: Pubkey,

    /// The track key.
    pub key: Hash,

    /// The size of the track in bytes.
    pub size: StorageUnits,

    /// The merkle root of the track data.
    pub root: Hash,

    /// Track data.
    pub data: BlobData,
}


state!(AccountType, Track);
