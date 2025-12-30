//! Pending operation column families for recovery and handoff queues

use crate::ops::{HandoffInfo, RecoveryInfo};
use crate::types::SliceKey;
use store::Column;

/// Pending recovery queue
/// Key: SliceKey { spool_idx: u16, track_address: Pubkey }
/// Value: RecoveryInfo
pub struct PendingRecover;

impl Column for PendingRecover {
    const CF_NAME: &'static str = "pending/recover";
    type Key = SliceKey;
    type Value = RecoveryInfo;
}

/// Pending handoff queue
/// Key: SliceKey { spool_idx: u16, track_address: Pubkey }
/// Value: HandoffInfo
pub struct PendingHandoff;

impl Column for PendingHandoff {
    const CF_NAME: &'static str = "pending/handoff";
    type Key = SliceKey;
    type Value = HandoffInfo;
}
