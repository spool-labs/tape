//! Garbage collection tracking column family

use crate::types::EpochNumber;
use store::Column;

/// GC progress tracking
///
/// Key: String ("started" or "completed")
/// Value: EpochNumber (last epoch where GC was started/completed)
///
/// Used to track GC progress across restarts.
pub struct Gc;

impl Column for Gc {
    const CF_NAME: &'static str = "gc";
    type Key = String;
    type Value = EpochNumber;
}
