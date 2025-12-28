//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.

mod committee;
mod slice;
mod stats;
mod tape;
mod track;

pub use committee::CommitteeData;
pub use slice::{
    AssignmentStatus, Compression, SliceMeta, SliceOps, SliceState, SliceStatus, SyncPhase,
    SyncProgress,
};
pub use stats::{StatsOps, StorageStats};
pub use tape::{TapeData, TapeOps};
pub use track::{TrackData, TrackOps};
