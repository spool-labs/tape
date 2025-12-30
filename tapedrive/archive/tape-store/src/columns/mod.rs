//! Column family definitions for tape-store
//!
//! This module defines 9 column families (down from 16):
//! - meta: Node configuration and metadata
//! - tracks: Minimal track info indexed by address
//! - slices/data: Slice blob data (BlobDB)
//! - slices/meta: Slice metadata with merkle proofs
//! - spools/assigned: Spool assignment tracking
//! - committee: Committee cache by epoch
//! - pending/recover: Recovery queue
//! - pending/handoff: Handoff queue
//! - gc/scheduled: Garbage collection index

pub mod committee;
pub mod gc;
pub mod meta;
pub mod pending;
pub mod slices;
pub mod spools;
pub mod tracks;

// Re-export all column types
pub use committee::Committee;
pub use gc::GcScheduled;
pub use meta::Meta;
pub use pending::{PendingHandoff, PendingRecover};
pub use slices::{SlicesData, SlicesMeta};
pub use spools::SpoolsAssigned;
pub use tracks::Tracks;

/// List of all column family names in the store (9 total)
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "tracks",
    "slices/data",
    "slices/meta",
    "spools/assigned",
    "committee",
    "pending/recover",
    "pending/handoff",
    "gc/scheduled",
];
