//! Sub-leaf tree nodes kept beside a slice so a challenge answers without rehashing it
//!
//! Key structure: (spool_id, track_address) - mirrors the slice column

use store::Column;
use tape_crypto::Hash;

use crate::types::SliceKey;

/// One sub-leaf tree node per sample window of a slice
///
/// Answering a storage challenge needs the sampled leaf's path to the slice root.
/// Rebuilding that path from the bytes costs a hash of the whole slice, which is
/// the opposite of the read-is-fast asymmetry the challenge relies on. Keeping the
/// nodes at one intermediate level turns it into a hash of one window: 1.2 KB of
/// index for a 9 MiB slice.
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: the level's nodes, in order
pub struct SliceSidecarCol;

impl Column for SliceSidecarCol {
    const CF_NAME: &'static str = "slice_sidecar";
    type Key = SliceKey;
    type Value = Vec<Hash>;
}
