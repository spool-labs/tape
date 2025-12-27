//! Slice column families for data, metadata, and state

use crate::types::{AssignmentStatus, SliceKey, SliceMeta, SliceState, SpoolKey, SyncProgress};
use store::Column;

/// Slice data storage (large values, should use BlobDB)
/// Key: SliceKey (track_id, spool_idx)
/// Value: Vec<u8> (slice data, up to 32 MiB)
pub struct SlicesData;

impl Column for SlicesData {
    const CF_NAME: &'static str = "slices/data";
    type Key = SliceKey;
    type Value = Vec<u8>;
}

/// Slice metadata
/// Key: SliceKey (track_id, spool_idx)
/// Value: SliceMeta
pub struct SlicesMeta;

impl Column for SlicesMeta {
    const CF_NAME: &'static str = "slices/meta";
    type Key = SliceKey;
    type Value = SliceMeta;
}

/// Slice state tracking
/// Key: SliceKey (track_id, spool_idx)
/// Value: SliceState
pub struct SlicesState;

impl Column for SlicesState {
    const CF_NAME: &'static str = "slices/state";
    type Key = SliceKey;
    type Value = SliceState;
}

/// Assignment status for each spool
/// Key: SpoolKey (spool_idx)
/// Value: AssignmentStatus
pub struct AssignmentStatusCF;

impl Column for AssignmentStatusCF {
    const CF_NAME: &'static str = "assignment/status";
    type Key = SpoolKey;
    type Value = AssignmentStatus;
}

/// Assignment sync progress for each spool
/// Key: SpoolKey (spool_idx)
/// Value: SyncProgress
pub struct AssignmentProgressCF;

impl Column for AssignmentProgressCF {
    const CF_NAME: &'static str = "assignment/progress";
    type Key = SpoolKey;
    type Value = SyncProgress;
}
