//! Column family definitions for tape-store
//!
//! This module defines 11 column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `tape`: Tape metadata (Pubkey -> TapeInfo)
//! - `track`: Track metadata (Pubkey -> TrackInfo)
//! - `object_info`: Object metadata (Pubkey -> ObjectInfo)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Spool Columns (NOT epoch-namespaced)
//! - `spool_status`: Spool status (SpoolIndexKey -> SpoolStatus)
//! - `spool_pending_recovery`: Pending recovery (SliceKey -> ())
//! - `spool_sync_cursor`: Sync cursor (SpoolIndexKey -> Pubkey)
//!
//! ## Slice Data Column (BlobDB)
//! - `slice`: Slice data (SliceKey -> Vec<u8>)
//!
//! ## Committee Column
//! - `committee`: Committee by epoch (EpochKey -> Vec<NodeInfo>)

pub mod committee;
pub mod gc;
pub mod meta;
pub mod object_info;
pub mod slice;
pub mod spool;
pub mod sync_cursor;
pub mod tape;
pub mod track;

// Re-export all column types
pub use committee::CommitteeCol;
pub use gc::GcCol;
pub use meta::MetaCol;
pub use object_info::ObjectInfoCol;
pub use slice::SliceCol;
pub use spool::{SpoolPendingRecoveryCol, SpoolStatusCol, SpoolSyncCursorCol};
pub use sync_cursor::SyncCursorCol;
pub use tape::TapeCol;
pub use track::TrackCol;

/// List of all column family names in the store (11 total)
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "committee",
    "tape",
    "track",
    "object_info",
    "sync_cursor",
    "gc",
    "spool_status",
    "spool_pending_recovery",
    "slice",
    "spool_sync_cursor",
];
