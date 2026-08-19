//! Where a tape store's families live, and how the rocks arm configures them
//!
//! The layout constants are engine-agnostic: the reel reads them to route a
//! family, and the bench arm reads them to place a column family on a volume.
//! Everything that names a RocksDB type lives in `rocks`, behind the feature of
//! that name, so a node's build never reaches the engine it does not run.

#[cfg(feature = "rocks")]
mod rocks;

#[cfg(feature = "rocks")]
pub use rocks::*;

/// Subdirectory of a store root holding the metadata (fast volume) database
pub const META_SUBDIR: &str = "meta";

/// Subdirectory of a store root holding the bulk (large volume) database
pub const BULK_SUBDIR: &str = "bulk";

/// Column families that hold bulk payloads and live on the bulk volume
///
/// The slice and snapshot families use key-value separation; track data is
/// stored inline but can be large. Everything else is small metadata that
/// stays on the fast volume. The slice size index is small, but it rides along
/// on the bulk volume because a write batch cannot span the two databases.
/// A slice, its recorded length and its sidecar are written in one batch, so
/// they have to share a volume: a cross-volume batch is not atomic.
pub const BULK_COLUMN_FAMILIES: &[&str] =
    &["track_data", "slice", "snapshot_artifact"];
