//! Spool column family for tracking assigned spools
//!
//! Tracks spools this node owns and their current state.

use crate::ops::SpoolState;
use crate::types::SpoolKey;
use store::Column;

/// Assigned spools tracking
/// Key: u16 (spool_idx)
/// Value: SpoolState
pub struct SpoolsAssigned;

impl Column for SpoolsAssigned {
    const CF_NAME: &'static str = "spools/assigned";
    type Key = SpoolKey;
    type Value = SpoolState;
}
