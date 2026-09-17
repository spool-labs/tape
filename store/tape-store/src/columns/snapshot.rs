//! Snapshot coordination columns.

use tape_store_api::Column;

use crate::types::{SnapshotArtifact, SnapshotArtifactKey};

/// Local snapshot build artifacts staged until the write is observed on-chain.
pub struct SnapshotArtifactCol;

impl Column for SnapshotArtifactCol {
    const CF_NAME: &'static str = "snapshot_artifact";
    type Key = SnapshotArtifactKey;
    type Value = SnapshotArtifact;
}
