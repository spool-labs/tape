#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::spooler::SpoolGroup;
use crate::track::blob::BlobInfo;
use crate::types::{TrackNumber, EpochNumber};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotInfo {

    /// The epoch this snapshot is for
    pub epoch: EpochNumber,

    /// The chunk info for each SpoolGroup. A snapshot is considered "certified" when all SpoolGroups are present
    pub chunks: Vec<SnapshotChunkInfo>,

    // The tape_address and manifest_address can be derived from the epoch, so we don't need to
    // store them here.
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
