//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.

mod committee;
mod slice;
mod spool;
mod stats;
mod track;

pub use committee::{CommitteeCache, CommitteeMemberInfo, CommitteeOps};
pub use slice::{Compression, SliceMeta, SliceOps, MERKLE_HEIGHT};
pub use spool::{HandoffInfo, RecoveryInfo, SpoolOps, SpoolState, SpoolStatus};
pub use stats::{StatsOps, StorageStats};
pub use track::{TrackInfo, TrackOps};
