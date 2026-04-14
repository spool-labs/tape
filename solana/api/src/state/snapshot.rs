use tape_core::prelude::*;
use tape_core::track::archive::TrackArchive;
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

    /// A merkle tree of compressed tracks that store the snapshot data
    pub tracks: TrackArchive,
}


impl Snapshot {
    pub fn write_track(&mut self, track: &CompressedTrack) -> ProgramResult {
        // Unlike a tape, we don't check or update the used storage for a snapshot, since snapshots
        // are not capacity-limited like tapes.

        self.tracks
            .append(track)
            .map_err(|_| ProgramError::InvalidInstructionData)?;

        Ok(())
    }
}

tape_solana::state!(AccountType, Snapshot);
