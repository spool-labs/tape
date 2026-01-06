use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Track {
    /// The unique identifier for this track.
    pub id: TrackNumber,

    /// The tape this track is stored on.
    pub tape: Pubkey,

    /// The track key.
    pub key: Hash,

    /// The size of the track in bytes.
    pub size: StorageUnits,

    /// The data associated with this track.
    pub data: TrackData,
}


tape_solana::state!(AccountType, Track);
