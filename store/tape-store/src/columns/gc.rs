//! Garbage collection tracking column family

use crate::types::EpochNumber;
use store::Column;

/// GC progress tracking
///
/// Key: String ("started" or "completed")
/// Value: EpochNumber (last epoch where GC was started/completed)
pub struct GcCol;

impl Column for GcCol {
    const CF_NAME: &'static str = "gc";
    type Key = String;
    type Value = EpochNumber;
}
