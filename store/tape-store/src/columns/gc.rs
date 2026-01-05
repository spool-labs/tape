//! Garbage collection index column family

use crate::types::GcKey;
use store::Column;

/// GC scheduled index for time-based deletion
/// Key: GcKey { timestamp: i64, spool_idx: u16, track_address: Pubkey }
/// Value: unit (presence in GC queue)
pub struct GcScheduled;

impl Column for GcScheduled {
    const CF_NAME: &'static str = "gc/scheduled";
    type Key = GcKey;
    type Value = ();
}
