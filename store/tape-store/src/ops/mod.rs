//! High-level operation traits for TapeStore
//!
//! ## Operation Traits
//!
//! - `MetaOps`: Node status, cluster hash, current epoch, node address, sync cursor, GC tracking
//! - `TapeOps`: Tape metadata
//! - `TrackOps`: Track metadata
//! - `ObjectInfoOps`: Object info (blacklisted, invalid, valid)
//! - `SpoolOps`: Spool status, sync progress, pending recovery (NOT epoch-namespaced)
//! - `SliceOps`: Slice data storage

mod event_log;
mod meta;
mod object_info;
mod slice;
mod spool;
mod tape;
mod track;

// Re-export operation traits
pub use event_log::EventLogOps;
pub use meta::MetaOps;
pub use object_info::ObjectInfoOps;
pub use slice::SliceOps;
pub use spool::SpoolOps;
pub use tape::TapeOps;
pub use track::TrackOps;
