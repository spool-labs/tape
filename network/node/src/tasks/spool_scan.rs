//! RecoveryScan — scan all tracks to find missing slices for a spool.
//!
//! After a spool transitions to Scan (from sync or from a local verification path),
//! the scheduler runs RecoveryScan. This task walks every track in the store, checks
//! whether the track belongs to this spool's group, and if the slice is missing,
//! enqueues it into the pending_recovery queue.
//!
//! On completion the scan decides the next state: if any pending recoveries exist,
//! the spool moves to Recover; otherwise it goes directly to Active.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use rpc::Rpc;
use store::Store;
use tape_api::prelude::SpoolIndex;
use tape_core::erasure::spool_in_group;
use tape_protocol::Api;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::{ObjectInfo, SpoolState, SpoolStatus};

use crate::core::NodeContext;
use crate::TaskOutcome;

const SCAN_BATCH_SIZE: usize = 100;

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    cancel: CancellationToken,
) -> TaskOutcome {
    let mut cursor = None;
    let mut any_errors = false;

    // Walk all tracks in batches. For each track that belongs to this spool's group, check
    // whether we already have the slice. If not, add it to the pending recovery queue for
    // SpoolRecovery to pick up.
    loop {
        if cancel.is_cancelled() {
            return TaskOutcome::Success;
        }

        let tracks = match context.store.iter_tracks_from(cursor, SCAN_BATCH_SIZE) {
            Ok(t) => t,
            Err(e) => return TaskOutcome::Retryable(format!("iter_tracks: {e}")),
        };

        if tracks.is_empty() {
            break;
        }

        for (track_addr, track_info) in &tracks {
            // Skip tracks that don't belong to this spool's group.
            if !spool_in_group(spool, track_info.spool_group) {
                tracing::warn!(?track_addr, spool, "track not in spool group");
                continue;
            }

            // Only recover tracks with valid object info — deleted, invalidated, or
            // unregistered tracks are not worth recovering.
            let recoverable = match context.store.get_object_info(*track_addr) {
                Ok(Some(ObjectInfo::Valid { .. })) => true,
                Ok(_) => false,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "get_object_info error: {e}");
                    any_errors = true;
                    continue;
                }
            };

            if !recoverable {
                continue;
            }

            // Already have the slice — nothing to recover.
            let has = match context.store.has_slice(spool, *track_addr) {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(?track_addr, spool, "has_slice error: {e}");
                    any_errors = true;
                    continue;
                }
            };

            if !has {
                if let Err(e) = context.store.add_pending_recovery(spool, *track_addr) {
                    tracing::warn!(?track_addr, spool, "add_pending_recovery error: {e}");
                    any_errors = true;
                }
            }
        }

        cursor = tracks.last().map(|(addr, _)| *addr);
    }

    // If any store operations failed, retry the whole scan. Partial progress is safe because
    // add_pending_recovery is idempotent — re-adding an already-queued track is a no-op.
    if any_errors {
        return TaskOutcome::Retryable("scan encountered store errors".into());
    }

    // Verify the spool is still in Scan state before transitioning.
    let current = match context.store.get_spool_state(spool) {
        Ok(Some(s)) if s.is_scanning() => s,
        Ok(_) => {
            tracing::warn!(spool, "spool no longer in Scan state, skipping transition");
            return TaskOutcome::Success;
        }
        Err(e) => return TaskOutcome::Retryable(format!("get_spool_state: {e}")),
    };

    // Decide next state: if pending recoveries exist, move to Recover; otherwise Active.
    let has_pending = match context.store.iter_pending_recoveries(spool, 1) {
        Ok(p) => !p.is_empty(),
        Err(e) => return TaskOutcome::Retryable(format!("iter_pending_recoveries: {e}")),
    };

    let new_state = if has_pending {
        SpoolState { status: SpoolStatus::Recover, ..current }
    } else {
        SpoolState::new(SpoolStatus::Active, current.epoch)
    };

    if let Err(e) = context.store.set_spool_state(spool, new_state) {
        return TaskOutcome::Retryable(format!("set_spool_state: {e}"));
    }

    tracing::info!(spool, ?new_state, "recovery scan complete");

    TaskOutcome::Success
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::spooler::SpoolGroup;
    use tape_core::types::EpochNumber;
    use tape_store::ops::{ObjectInfoOps, SpoolOps, TrackOps};
    use tape_store::types::{Pubkey as StorePubkey, SpoolState, SpoolStatus, TrackInfo};
    use tokio_util::sync::CancellationToken;

    use crate::core::test_utils::test_context;

    const SPOOL: SpoolIndex = 5;

    fn scan_state() -> SpoolState {
        SpoolState::new(SpoolStatus::Scan, EpochNumber(3))
    }

    fn track_addr(n: u8) -> StorePubkey {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        StorePubkey(bytes)
    }

    fn track_in_group() -> TrackInfo {
        TrackInfo {
            tape_address: StorePubkey::new_unique(),
            spool_group: SpoolGroup::of(SPOOL),
            original_size: 1024,
            stripe_size: 1024,
            stripe_count: 1,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    fn track_wrong_group() -> TrackInfo {
        // SPOOL=5 is in group 0; spool 20 is in group 1.
        TrackInfo {
            tape_address: StorePubkey::new_unique(),
            spool_group: SpoolGroup::of(20),
            original_size: 1024,
            stripe_size: 1024,
            stripe_count: 1,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    fn valid_object(addr: StorePubkey) -> ObjectInfo {
        ObjectInfo::Valid {
            is_stored: true,
            track_address: addr,
            registered_epoch: EpochNumber(1),
            certified_epoch: None,
            slot: tape_core::types::SlotNumber(0),
        }
    }

    #[tokio::test]
    async fn scan_empty_store() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(ctx.store.get_spool_state(SPOOL).unwrap().unwrap().is_active());
    }

    #[tokio::test]
    async fn scan_enqueues_missing() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        ctx.store.put_object_info(addr, valid_object(addr)).unwrap();
        // No slice stored — should be enqueued.

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(ctx.store.has_pending_recovery(SPOOL, addr).unwrap());
        assert!(ctx.store.get_spool_state(SPOOL).unwrap().unwrap().is_recovering());
    }

    #[tokio::test]
    async fn scan_skips_existing() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        ctx.store.put_object_info(addr, valid_object(addr)).unwrap();
        ctx.store.put_slice(SPOOL, addr, vec![0xAA; 32]).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(!ctx.store.has_pending_recovery(SPOOL, addr).unwrap());
    }

    #[tokio::test]
    async fn scan_skips_wrong_group() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_wrong_group()).unwrap();
        ctx.store.put_object_info(addr, valid_object(addr)).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(!ctx.store.has_pending_recovery(SPOOL, addr).unwrap());
    }

    #[tokio::test]
    async fn scan_skips_invalid_object() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        ctx.store.put_object_info(addr, ObjectInfo::Invalid {
            epoch: EpochNumber(1),
            slot: tape_core::types::SlotNumber(0),
        }).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(!ctx.store.has_pending_recovery(SPOOL, addr).unwrap());
    }

    #[tokio::test]
    async fn scan_skips_no_object_info() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        // No object info set.

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));
        assert!(!ctx.store.has_pending_recovery(SPOOL, addr).unwrap());
    }

    #[tokio::test]
    async fn scan_cancel() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        ctx.store.put_object_info(addr, valid_object(addr)).unwrap();

        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run(ctx.clone(), SPOOL, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        // State should remain Scan on cancel.
        assert!(ctx.store.get_spool_state(SPOOL).unwrap().unwrap().is_scanning());
    }

    #[tokio::test]
    async fn scan_idempotent() {
        let ctx = test_context();
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let addr = track_addr(1);
        ctx.store.put_track(addr, track_in_group()).unwrap();
        ctx.store.put_object_info(addr, valid_object(addr)).unwrap();

        let r1 = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(r1, TaskOutcome::Success));
        assert!(ctx.store.get_spool_state(SPOOL).unwrap().unwrap().is_recovering());

        // Reset to Scan so the second run actually scans again.
        ctx.store.set_spool_state(SPOOL, scan_state()).unwrap();

        let r2 = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(r2, TaskOutcome::Success));

        // Still exactly one pending recovery entry.
        let pending = ctx.store.iter_pending_recoveries(SPOOL, 100).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], addr);
    }
}
