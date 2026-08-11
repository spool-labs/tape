use store::Column;
use tape_crypto::Hash;

use crate::types::SliceKey;

/// Stores intermediate tree nodes used to prove a sampled slice window.
pub struct SliceSidecarCol;

impl Column for SliceSidecarCol {
    const CF_NAME: &'static str = "slice_sidecar";
    type Key = SliceKey;
    type Value = Vec<Hash>;
}
