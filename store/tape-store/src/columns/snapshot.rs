//! Snapshot metadata and staging columns.

use crate::types::{EpochKey, SliceValue, SnapshotGroupKey, SnapshotSliceKey};
use store::Column;
use tape_core::snapshot::info::{SnapshotEpochInfo, SnapshotGroupInfo};

pub struct SnapshotEpochCol;
pub struct SnapshotGroupCol;
pub struct SnapshotSliceCol;

impl Column for SnapshotEpochCol {
    const CF_NAME: &'static str = "snapshot_epoch";
    type Key = EpochKey;
    type Value = SnapshotEpochInfo;
}

impl Column for SnapshotGroupCol {
    const CF_NAME: &'static str = "snapshot_group";
    type Key = SnapshotGroupKey;
    type Value = SnapshotGroupInfo;
}

impl Column for SnapshotSliceCol {
    const CF_NAME: &'static str = "snapshot_slice";
    type Key = SnapshotSliceKey;
    type Value = SliceValue;
}
