//! Snapshot coordination columns.

use store::Column;
use tape_core::bls::BlsSignature;

use crate::types::{
    SnapshotArtifact, SnapshotArtifactKey, SnapshotFinalizeSigKey, SnapshotWriteSigKey,
};

/// Per-chunk pushed write partial signatures.
pub struct SnapshotWriteSigCol;

impl Column for SnapshotWriteSigCol {
    const CF_NAME: &'static str = "snapshot_write_sig";
    type Key = SnapshotWriteSigKey;
    type Value = BlsSignature;
}

/// Per-group pushed finalize partial signatures.
pub struct SnapshotFinalizeSigCol;

impl Column for SnapshotFinalizeSigCol {
    const CF_NAME: &'static str = "snapshot_finalize_sig";
    type Key = SnapshotFinalizeSigKey;
    type Value = BlsSignature;
}

/// Local snapshot build artifacts staged until the write is observed on-chain.
pub struct SnapshotArtifactCol;

impl Column for SnapshotArtifactCol {
    const CF_NAME: &'static str = "snapshot_artifact";
    type Key = SnapshotArtifactKey;
    type Value = SnapshotArtifact;
}
