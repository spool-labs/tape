//! Slice payload size index
//!
//! Key structure: (spool_id, track_address) - mirrors the slice column

use store::Column;
use tape_core::types::StorageUnits;

use crate::types::SliceKey;

/// Slice payload lengths, kept apart so totals never read the blob files
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: StorageUnits (payload length in bytes)
pub struct SliceSizeCol;

impl Column for SliceSizeCol {
    const CF_NAME: &'static str = "slice_size";
    type Key = SliceKey;
    type Value = StorageUnits;
}
