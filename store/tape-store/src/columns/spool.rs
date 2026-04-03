//! Spool column families for spool tracking (NOT epoch-namespaced)
//!
//! - SpoolStatusCol: spool_id -> SpoolStatus
//! - SpoolPendingRepairCol: (spool_id, track_address) -> ()
//! - SpoolPendingRecoveryCol: (spool_id, track_address) -> ()
//! - SpoolSyncCursorCol: spool_id -> Address (last synced track)

use store::Column;
use tape_crypto::address::Address;

use crate::types::{SliceKey, SpoolIndexKey, SpoolState};

/// Spool status tracking
///
/// Key: SpoolIndexKey (2 bytes: spool_id BE)
/// Value: SpoolState (status + epoch entered)
pub struct SpoolStatusCol;

impl Column for SpoolStatusCol {
    const CF_NAME: &'static str = "spool_status";
    type Key = SpoolIndexKey;
    type Value = SpoolState;
}

/// Pending repair queue (presence-only)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: () (presence indicates pending)
pub struct SpoolPendingRepairCol;

impl Column for SpoolPendingRepairCol {
    const CF_NAME: &'static str = "spool_pending_repair";
    type Key = SliceKey;
    type Value = ();
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

/// Spool sync cursor tracking
///
/// Key: SpoolIndexKey (2 bytes: spool_id BE)
/// Value: Address (last synced track address)
pub struct SpoolSyncCursorCol;

impl Column for SpoolSyncCursorCol {
    const CF_NAME: &'static str = "spool_sync_cursor";
    type Key = SpoolIndexKey;
    type Value = Address;
}
