//! Snapshot metadata and staging columns.

use crate::types::{EpochKey, SliceValue, SnapshotSliceKey};
use store::Column;
use tape_core::snapshot::types::SnapshotInfo;

pub struct SnapshotCol;
pub struct SnapshotSliceCol;

impl Column for SnapshotCol {
    const CF_NAME: &'static str = "snapshot";
    type Key = EpochKey;
    type Value = SnapshotInfo;
}

impl Column for SnapshotSliceCol {
    const CF_NAME: &'static str = "snapshot_slice";
    type Key = SnapshotSliceKey;
    type Value = SliceValue;
}
