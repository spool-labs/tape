//! Slice data column families for primary and recovery slices
//!
//! Key structure: (spool_id, track_address) - enables efficient iteration by spool

use crate::types::{PrimarySliceData, RecoverySliceData, SliceKey};
use store::Column;

/// Primary slice data storage (large values, uses BlobDB)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: PrimarySliceData (symbols + padding info, typically ~1MB)
pub struct PrimarySlices;

impl Column for PrimarySlices {
    const CF_NAME: &'static str = "primary_slices";
    type Key = SliceKey;
    type Value = PrimarySliceData;
}

/// Recovery slice data storage (large values, uses BlobDB)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: RecoverySliceData (packed column symbols, typically ~1MB)
///
/// Each recovery column contains parts from all 1024 primary slices,
/// enabling reconstruction of any missing primary slice.
pub struct RecoverySlices;

impl Column for RecoverySlices {
    const CF_NAME: &'static str = "recovery_slices";
    type Key = SliceKey;
    type Value = RecoverySliceData;
}
