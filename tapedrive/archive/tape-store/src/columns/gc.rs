//! Garbage collection index column family

use crate::types::GcKey;
use store::Column;

/// GC index for time-based deletion
/// Key: GcKey (gc_at, track_id, spool_idx)
/// Value: unit (presence in GC queue)
pub struct GcIndex;

impl Column for GcIndex {
    const CF_NAME: &'static str = "gc_index";
    type Key = GcKey;
    type Value = ();
}
