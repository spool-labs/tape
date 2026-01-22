//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.
//!
//! ## Operation Traits
//!
//! - `MetaOps`: Node status, cluster hash, current epoch, sync cursor, GC tracking
//! - `SliceInfoOps`: Blob erasure coding metadata (hashes for verification)
//! - `TapeInfoOps`: Tape (storage allocation) metadata
//! - `TrackInfoOps`: Track (blob) metadata and certification
//! - `SpoolOps`: Epoch-namespaced spool status, sync progress, pending recovery
//! - `SliceDataOps`: Primary and recovery slice data storage
//! - `CommitteeOps`: Committee cache by epoch

mod committee;
mod meta;
mod slice_data;
mod slice_info;
mod spool;
mod tape_info;
mod track_info;

// Re-export operation traits
pub use committee::CommitteeOps;
pub use meta::MetaOps;
pub use slice_data::SliceDataOps;
pub use slice_info::SliceInfoOps;
pub use spool::SpoolOps;
pub use tape_info::TapeInfoOps;
pub use track_info::TrackInfoOps;
