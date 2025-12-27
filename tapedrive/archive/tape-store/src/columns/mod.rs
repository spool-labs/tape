//! Column family definitions for tape-store

pub mod committee;
pub mod gc;
pub mod meta;
pub mod recovery;
pub mod slices;
pub mod tapes;
pub mod tracks;

// Re-export all column types
pub use committee::CommitteeByEpoch;
pub use gc::GcIndex;
pub use meta::Meta;
pub use recovery::PendingRecover;
pub use slices::{AssignmentProgressCF, AssignmentStatusCF, SlicesData, SlicesMeta, SlicesState};
pub use tapes::{TapesActiveIndex, TapesById, TapesByAddress};
pub use tracks::{TracksByAddress, TracksByBlobKey, TracksById, TracksByTape};

/// List of all column family names in the store
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "tapes/by_id",
    "tapes/by_address",
    "tapes/active_index",
    "tracks/by_id",
    "tracks/by_address",
    "tracks/by_tape",
    "tracks/by_blob_key",
    "slices/data",
    "slices/meta",
    "slices/state",
    "assignment/status",
    "assignment/progress",
    "committee/by_epoch",
    "pending_recover",
    "gc_index",
];
