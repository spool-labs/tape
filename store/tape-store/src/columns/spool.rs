//! Spool column families for epoch-namespaced spool tracking
//!
//! These columns use epoch-first keys for crash-safe epoch transitions:
//! - SpoolAssigned: (epoch, spool_id) -> SpoolStatus
//! - SpoolSyncProgress: (epoch, spool_id) -> SyncProgress
//! - SpoolPendingRecovery: (epoch, spool_id, slice_type, track) -> ()

use crate::types::{PendingRecoveryKey, SpoolEpochKey, SpoolStatus, SyncProgress};
use store::Column;

/// Epoch-namespaced spool assignment tracking
///
/// Key: SpoolEpochKey (10 bytes: epoch BE + spool_id BE)
/// Value: SpoolStatus
///
/// Epoch-first ordering enables efficient cleanup of old epoch data.
pub struct SpoolAssigned;

impl Column for SpoolAssigned {
    const CF_NAME: &'static str = "spool_status";
    type Key = SpoolEpochKey;
    type Value = SpoolStatus;
}

/// Epoch-namespaced sync progress tracking
///
/// Key: SpoolEpochKey (10 bytes: epoch BE + spool_id BE)
/// Value: SyncProgress (last synced track, slice type)
pub struct SpoolSyncProgress;

impl Column for SpoolSyncProgress {
    const CF_NAME: &'static str = "sync_cursors";
    type Key = SpoolEpochKey;
    type Value = SyncProgress;
}

/// Epoch-namespaced pending recovery queue
///
/// Key: PendingRecoveryKey (43 bytes: epoch + spool_id + slice_type + track_address)
/// Value: () (presence indicates pending)
///
/// Stores slices that need to be recovered. The value is empty since
/// the key contains all necessary information.
pub struct SpoolPendingRecovery;

impl Column for SpoolPendingRecovery {
    const CF_NAME: &'static str = "recovery_queue";
    type Key = PendingRecoveryKey;
    type Value = ();
}
