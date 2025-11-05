use steel::*;
use core::marker::PhantomData;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Track {
    /// The tape this track is stored on.
    pub tape: Pubkey,

    /// The track number.
    pub number: TrackNumber,

    /// The track kind.
    pub kind: u64,

    /// The size of the track in bytes.
    pub size: StorageUnits,

    /// The merkle root of the raw data.
    pub root: Hash,

    /// Track metadata, either of StreamData or BlobData size.
    pub data: PhantomData<[u8]>,
}


state!(AccountType, Track);
