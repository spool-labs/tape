//! Slice column families for data and metadata
//!
//! Key structure: (spool_idx, track_address) - enables efficient iteration by spool

use crate::ops::SliceMeta;
use crate::types::SliceKey;
use store::Column;

/// Slice data storage (large values, uses BlobDB)
/// Key: SliceKey { spool_idx: u16, track_address: Pubkey }
/// Value: Vec<u8> (compressed slice data, up to MAX_SLICE_SIZE)
pub struct SlicesData;

impl Column for SlicesData {
    const CF_NAME: &'static str = "slices/data";
    type Key = SliceKey;
    type Value = Vec<u8>;
}

/// Slice metadata
/// Key: SliceKey { spool_idx: u16, track_address: Pubkey }
/// Value: SliceMeta
pub struct SlicesMeta;

impl Column for SlicesMeta {
    const CF_NAME: &'static str = "slices/meta";
    type Key = SliceKey;
    type Value = SliceMeta;
}
