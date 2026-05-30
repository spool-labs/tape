//! Network-free snapshot reader.
//!
//! Given a snapshot tape's chunk-track list and the fetched Clay/outer slices,
//! this crate verifies the track set against the committed track-merkle root and
//! decodes the slices back into a [`SnapshotLog`]. It carries no encoder and no
//! consensus logic, so it is shared by the node (bootstrap) and external clients
//! (epoch explorer, lite clients). Callers supply their own transport.

mod decode;
mod verify;

pub use decode::{
    assemble_snapshot_log, decode_chunk_payload, snapshot_track_group_count,
    validate_snapshot_track_list, K_INNER,
};
pub use verify::verify_snapshot_track_set;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot tape for epoch {epoch} has no tracks")]
    EmptyTrackList { epoch: u64 },

    #[error("snapshot track for epoch {epoch} references the wrong tape")]
    WrongTape { epoch: u64 },

    #[error("snapshot track for epoch {epoch} is not a blob track")]
    NonBlobTrack { epoch: u64 },

    #[error("snapshot for epoch {epoch} has no groups")]
    NoGroups { epoch: u64 },

    #[error("snapshot for epoch {epoch} has no decoded chunks")]
    NoChunks { epoch: u64 },

    #[error("snapshot track set has {got} tracks, committed root expects {expected}")]
    TrackCountMismatch { expected: u64, got: usize },

    #[error("snapshot track numbers are not contiguous from zero")]
    Contiguity,

    #[error("snapshot track set does not match the committed merkle root")]
    RootMismatch,

    #[error("clay decode failed: {0}")]
    ClayDecode(String),

    #[error("outer rs decode failed: {0}")]
    OuterDecode(String),

    #[error("snapshot chunk payload failed: {0}")]
    ChunkPayload(String),

    #[error("only {got}/{need} groups decoded for epoch {epoch} chunk {chunk}")]
    InsufficientGroups {
        epoch: u64,
        chunk: u64,
        got: usize,
        need: usize,
    },

    #[error("missing decoded chunk {chunk} for epoch {epoch}")]
    MissingChunk { epoch: u64, chunk: usize },

    #[error("lz4 decompress failed: {0}")]
    Decompress(String),

    #[error("snapshot log deserialize failed: {0}")]
    Deserialize(String),

    #[error("decoded snapshot epoch mismatch: expected {expected}, got {got}")]
    EpochMismatch { expected: u64, got: u64 },
}
