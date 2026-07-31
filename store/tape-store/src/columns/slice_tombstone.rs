//! What remains of deleted slices, for the challenge sample set
//!
//! Key structure: [spool_id BE 2 bytes][track_address 32 bytes]

use store::Column;

use crate::types::{SliceKey, SliceTombstone};

/// Length and deletion slot of a slice whose payload is gone
///
/// The sample set for a round is fixed at the round window's base slot, so a
/// slice deleted after that base must still enumerate for that round. Both
/// values are chain-derived and every observer writes the same row. Swept once
/// no round can reference the deletion any more.
///
/// Key: spool id and track address (34 bytes)
/// Value: deletion slot and slice length
pub struct SliceTombstoneCol;

impl Column for SliceTombstoneCol {
    const CF_NAME: &'static str = "slice_tombstone";
    type Key = SliceKey;
    type Value = SliceTombstone;
}
