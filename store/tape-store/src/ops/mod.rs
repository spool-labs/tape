//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.

mod committee;
mod gc;
mod handoff;
mod meta;
mod recovery;
mod slice;
mod spool;
mod stats;
mod track;

pub use committee::{CommitteeCache, CommitteeMemberInfo, CommitteeOps};
pub use gc::{delete_track_data, run_epoch_gc, GcEntry, GcOps, GcReason, GcStats};
pub use handoff::{HandoffInfo, HandoffOps};
pub use meta::MetaOps;
pub use recovery::{backoff_delay_secs, is_ready_for_retry, RecoveryInfo, RecoveryOps};
pub use slice::{SliceMeta, SliceOps, MERKLE_HEIGHT};
pub use spool::{SpoolOps, SpoolState, SpoolStatus};
pub use stats::{StatsOps, StorageStats};
pub use track::{TrackInfo, TrackOps};
