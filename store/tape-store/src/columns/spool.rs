//! Spool column families for spool tracking (NOT epoch-namespaced)
//!
//! - SpoolStatusCol: spool_id -> SpoolStatus
//! - SpoolPendingRepairCol: (spool_id, track_address) -> ready slot
//! - SpoolPendingRecoveryCol: (spool_id, track_address) -> ()
//! - SpoolSyncCursorCol: spool_id -> the peer's opaque sweep mark

use tape_store_api::Column;
use tape_core::system::SpoolState;
use tape_core::types::SlotNumber;

use crate::types::{SliceKey, SpoolIndexKey};

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

/// Pending repair queue
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: the first slot the repair may run in, so a deferred row survives a restart
pub struct SpoolPendingRepairCol;

impl Column for SpoolPendingRepairCol {
    const CF_NAME: &'static str = "spool_pending_repair";
    type Key = SliceKey;
    type Value = SlotNumber;
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
/// Value: the peer's opaque sweep mark, which this node only ever hands back
///
/// A stored mark outlives the process that minted it and can meet a peer that
/// has restarted or a different peer entirely, which is what the mark's own
/// nonce is for: one the answering node did not mint restarts the sync rather
/// than resuming into a layout that is not there.
pub struct SpoolSyncCursorCol;

impl Column for SpoolSyncCursorCol {
    const CF_NAME: &'static str = "spool_sync_cursor";
    type Key = SpoolIndexKey;
    type Value = Vec<u8>;
}
