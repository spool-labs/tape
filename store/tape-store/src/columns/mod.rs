//! Column family definitions for tape-store
//!
//! This module defines the active column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `tape`: Tape metadata (Address -> TapeInfo)
//! - `track`: Canonical compressed-track catalog (Address -> PackedTrack)
//! - `track_lookup`: Tape-local ordered index ((tape, track_number, key) -> ())
//! - `track_data`: Local track payload data (Address -> TrackData)
//! - `object_info`: Object metadata (Address -> ObjectInfo)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Spool Columns (NOT epoch-namespaced)
//! - `spool_status`: Spool status (SpoolIndexKey -> SpoolStatus)
//! - `spool_pending_repair`: Pending repair (SliceKey -> ())
//! - `spool_pending_recovery`: Pending recovery (SliceKey -> ())
//! - `spool_sync_cursor`: Sync cursor (SpoolIndexKey -> Address)
//!
//! ## Slice Data Column (BlobDB)
//! - `slice`: Slice data (SliceKey -> Vec<u8>)
//!
//! ## Event Log Column
//! - `event_log`: Per-epoch replayable events (EventLogKey -> CapturedEvent)
//!
//! ## Vote Coordination Columns
//! - `vote_sig`: Per-group BLS signatures keyed by vote candidate and signer
//!
//! ## Snapshot Coordination Columns
//! - `snapshot_artifact`: Local build artifacts retained until snapshot finalization

pub mod event_log;
pub mod gc;
pub mod meta;
pub mod object_info;
pub mod snapshot;
pub mod slice;
pub mod spool;
pub mod sync_cursor;
pub mod tape;
pub mod track;
pub mod track_data;
pub mod track_lookup;
pub mod vote;

// Re-export all column types
pub use event_log::EventLogCol;
pub use gc::GcCol;
pub use meta::MetaCol;
pub use object_info::ObjectInfoCol;
pub use snapshot::SnapshotArtifactCol;
pub use slice::SliceCol;
pub use spool::{
    SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolStatusCol, SpoolSyncCursorCol,
};
pub use sync_cursor::SyncCursorCol;
pub use tape::TapeCol;
pub use track::TrackCol;
pub use track_data::TrackDataCol;
pub use track_lookup::TrackLookupCol;
pub use vote::VoteSigCol;

/// List of all column family names in the store.
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "tape",
    "track",
    "track_lookup",
    "track_data",
    "object_info",
    "sync_cursor",
    "gc",
    "spool_status",
    "spool_pending_repair",
    "spool_pending_recovery",
    "slice",
    "spool_sync_cursor",
    "event_log",
    "vote_sig",
    "snapshot_artifact",
];
