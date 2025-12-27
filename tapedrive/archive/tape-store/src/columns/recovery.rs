//! Recovery queue column family

use crate::types::RecoveryKey;
use store::Column;

/// Pending recovery queue
/// Key: RecoveryKey (spool_idx, track_id)
/// Value: unit (presence in queue)
pub struct PendingRecover;

impl Column for PendingRecover {
    const CF_NAME: &'static str = "pending_recover";
    type Key = RecoveryKey;
    type Value = ();
}
