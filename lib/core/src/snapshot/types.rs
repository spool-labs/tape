use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::spooler::SpoolGroup;
use crate::track::blob::BlobInfo;
use crate::types::{TrackNumber, EpochNumber};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum SnapshotState {
    Registered = 0,
    PartiallyCertified,
    Finalized,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotInfo {
    /// The epoch this snapshot is for
    pub epoch: EpochNumber,
    /// The chunk info for each SpoolGroup
    pub chunks: Vec<SnapshotChunkInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotChunkInfo {
    /// The TrackNumber that contains the snapshot blob info
    pub track: TrackNumber,
    /// The SpoolGroup that signed this snapshot
    pub group: SpoolGroup,
    /// The blob info for the snapshot chunk 
    pub blob: BlobInfo,
}
