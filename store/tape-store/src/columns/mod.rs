//! Column family definitions for tape-store
//!
//! This module defines 17 column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `tape`: Tape metadata (Pubkey -> TapeInfo)
//! - `track`: Canonical compressed-track catalog (Pubkey -> PackedTrack)
//! - `track_lookup`: Tape-local ordered index ((tape, track_number, key) -> ())
//! - `track_data`: Local track payload data (Pubkey -> TrackData)
//! - `object_info`: Object metadata (Pubkey -> ObjectInfo)
//! - `snapshot_epoch`: Per-epoch snapshot progress (EpochKey -> SnapshotEpochInfo)
//! - `snapshot_group`: Per-group snapshot artifacts (SnapshotGroupKey -> SnapshotGroupInfo)
//! - `snapshot_slice`: Staging snapshot slice bytes (SnapshotSliceKey -> SliceValue)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Spool Columns (NOT epoch-namespaced)
//! - `spool_status`: Spool status (SpoolIndexKey -> SpoolStatus)
//! - `spool_pending_repair`: Pending repair (SliceKey -> ())
//! - `spool_pending_recovery`: Pending recovery (SliceKey -> ())
//! - `spool_sync_cursor`: Sync cursor (SpoolIndexKey -> Pubkey)
//!
//! ## Slice Data Column (BlobDB)
//! - `slice`: Slice data (SliceKey -> Vec<u8>)

pub mod event_log;
pub mod gc;
pub mod meta;
pub mod object_info;
pub mod slice;
pub mod spool;
pub mod snapshot;
pub mod sync_cursor;
pub mod tape;
pub mod track;
pub mod track_lookup;
pub mod track_data;

// Re-export all column types
pub use event_log::EventLogCol;
pub use gc::GcCol;
pub use meta::MetaCol;
pub use object_info::ObjectInfoCol;
pub use slice::SliceCol;
pub use spool::{
    SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolStatusCol, SpoolSyncCursorCol,
};
pub use snapshot::{SnapshotEpochCol, SnapshotGroupCol, SnapshotSliceCol};
pub use sync_cursor::SyncCursorCol;
pub use tape::TapeCol;
pub use track::TrackCol;
pub use track_lookup::TrackLookupCol;
pub use track_data::TrackDataCol;

/// List of all column family names in the store (17 total)
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "tape",
    "track",
    "track_lookup",
    "track_data",
    "object_info",
    "snapshot_epoch",
    "snapshot_group",
    "snapshot_slice",
    "sync_cursor",
    "gc",
    "spool_status",
    "spool_pending_repair",
    "spool_pending_recovery",
    "slice",
    "spool_sync_cursor",
    "event_log",
];
