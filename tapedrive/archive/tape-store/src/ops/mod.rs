//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.

mod tape;
mod track;
mod slice;
mod stats;

// Re-export all public traits and types
pub use tape::TapeOps;
pub use track::TrackOps;
pub use slice::SliceOps;
pub use stats::{StatsOps, StorageStats};
