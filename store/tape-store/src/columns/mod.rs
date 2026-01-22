//! Column family definitions for tape-store
//!
//! This module defines 12 column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `slice_info`: Blob erasure coding metadata (Pubkey -> SliceInfo)
//! - `tape_info`: Tape (storage allocation) metadata (Pubkey -> TapeInfo)
//! - `track_info`: Track (blob) metadata (Pubkey -> TrackInfo)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Epoch-Namespaced Spool Columns
//! - `spool_status`: Spool status per epoch (SpoolEpochKey -> SpoolStatus)
//! - `sync_cursors`: Sync cursors per epoch (SpoolEpochKey -> SyncProgress)
//! - `recovery_queue`: Pending recovery queue (PendingRecoveryKey -> ())
//!
//! ## Slice Data Columns (BlobDB)
//! - `primary_slices`: Primary slice data (SliceKey -> PrimarySliceData)
//! - `recovery_slices`: Recovery slice data (SliceKey -> RecoverySliceData)
//!
//! ## Committee Column
//! - `committee`: Committee cache by epoch (EpochKey -> CommitteeCache)

pub mod committee;
pub mod cursor;
pub mod gc;
pub mod meta;
pub mod slice_info;
pub mod slices;
pub mod spool;
pub mod tape_info;
pub mod track_info;

// Re-export all column types
pub use committee::Committee;
pub use cursor::SyncCursor;
pub use gc::Gc;
pub use meta::Meta;
pub use slice_info::SliceInfoCol;
pub use slices::{PrimarySlices, RecoverySlices};
pub use spool::{SpoolAssigned, SpoolPendingRecovery, SpoolSyncProgress};
pub use tape_info::TapeInfoCol;
pub use track_info::TrackInfoCol;

/// List of all column family names in the store (12 total)
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "slice_info",
    "tape_info",
    "track_info",
    "sync_cursor",
    "gc",
    "spool_status",
    "sync_cursors",
    "recovery_queue",
    "primary_slices",
    "recovery_slices",
    "committee",
];
