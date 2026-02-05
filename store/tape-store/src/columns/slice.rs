//! Slice data column family (merged primary + recovery)
//!
//! Key structure: (spool_id, track_address) - enables efficient iteration by spool

use crate::types::SliceKey;
use store::Column;

/// Slice data storage (large values, uses BlobDB)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: Vec<u8> (raw slice data, typically ~1MB)
pub struct SliceCol;

impl Column for SliceCol {
    const CF_NAME: &'static str = "slice";
    type Key = SliceKey;
    type Value = Vec<u8>;
}
