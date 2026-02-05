//! Spool column families for spool tracking (NOT epoch-namespaced)
//!
//! - SpoolStatusCol: spool_id -> SpoolStatus
//! - SpoolPendingRecoveryCol: (spool_id, track_address) -> ()
//! - SpoolSyncProgressCol: spool_id -> Pubkey (last synced track)

use crate::types::{Pubkey, SliceKey, SpoolIndexKey, SpoolStatus};
use store::Column;

/// Spool status tracking
///
/// Key: SpoolIndexKey (2 bytes: spool_id BE)
/// Value: SpoolStatus
pub struct SpoolStatusCol;

impl Column for SpoolStatusCol {
    const CF_NAME: &'static str = "spool_status";
    type Key = SpoolIndexKey;
    type Value = SpoolStatus;
}

/// Pending recovery queue (presence-only)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: () (presence indicates pending)
pub struct SpoolPendingRecoveryCol;

impl Column for SpoolPendingRecoveryCol {
    const CF_NAME: &'static str = "spool_pending_recovery";
    type Key = SliceKey;
    type Value = ();
}

/// Spool sync progress tracking
///
/// Key: SpoolIndexKey (2 bytes: spool_id BE)
/// Value: Pubkey (last synced track address)
pub struct SpoolSyncProgressCol;

impl Column for SpoolSyncProgressCol {
    const CF_NAME: &'static str = "spool_sync_progress";
    type Key = SpoolIndexKey;
    type Value = Pubkey;
}
