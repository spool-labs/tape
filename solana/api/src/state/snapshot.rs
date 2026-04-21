use tape_core::prelude::*;
use tape_core::types::GroupBitmap;
use tape_solana::*;
use tape_crypto::Hash;

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
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Chunk {
    /// The epoch for which this chunk belongs to
    pub epoch: EpochNumber,

    /// The SpoolGroup that contributed this chunk
    pub group: SpoolGroup,

    /// The index of this chunk within the snapshot data
    pub chunk: ChunkNumber,

    /// The index of the track on the snapshot tape where this chunk is stored
    pub track: TrackNumber,

    /// The signed hash of the snapshot chunk BlobInfo.
    pub value_hash: Hash,
}

tape_solana::state!(AccountType, Snapshot);
tape_solana::state!(AccountType, Chunk);
