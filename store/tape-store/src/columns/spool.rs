//! Spool column families for spool tracking (NOT epoch-namespaced)
//!
//! - SpoolStatusCol: spool_id -> SpoolStatus
//! - SpoolPendingRepairCol: (spool_id, ready slot, track_address) -> ()
//! - SpoolRepairIndexCol: (spool_id, track_address) -> ready slot
//! - SpoolPendingRecoveryCol: (spool_id, ready slot, track_address) -> ()
//! - SpoolRecoveryIndexCol: (spool_id, track_address) -> ready slot

use tape_store_api::Column;
use tape_core::system::SpoolState;
use tape_core::types::SlotNumber;

use crate::types::{QueueKey, SliceKey, SpoolIndexKey};

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

/// Pending repair queue, in the order its rows are ready
///
/// Key: QueueKey (42 bytes: spool_id BE + ready slot BE + track_address)
/// Value: () (the key carries the row)
pub struct SpoolPendingRepairCol;

impl Column for SpoolPendingRepairCol {
    const CF_NAME: &'static str = "spool_pending_repair";
    type Key = QueueKey;
    type Value = ();
}

/// Each pending repair's ready slot, by track
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: the ready slot in the row's queue key
pub struct SpoolRepairIndexCol;

impl Column for SpoolRepairIndexCol {
    const CF_NAME: &'static str = "spool_repair_index";
    type Key = SliceKey;
    type Value = SlotNumber;
}

/// Pending recovery queue, in the order its rows are ready
///
/// Key: QueueKey (42 bytes: spool_id BE + ready slot BE + track_address)
/// Value: () (the key carries the row)
pub struct SpoolPendingRecoveryCol;

impl Column for SpoolPendingRecoveryCol {
    const CF_NAME: &'static str = "spool_pending_recovery";
    type Key = QueueKey;
    type Value = ();
}

/// Each pending recovery's ready slot, by track
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: the ready slot in the row's queue key
pub struct SpoolRecoveryIndexCol;

impl Column for SpoolRecoveryIndexCol {
    const CF_NAME: &'static str = "spool_recovery_index";
    type Key = SliceKey;
    type Value = SlotNumber;
}
