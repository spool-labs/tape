//! SpoolSync — sync spool data from a peer that previously owned it.

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_protocol::api::ApiError;
use tape_protocol::api::SyncReq;
use tape_retry::Retryable;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::types::SpoolState;

use crate::core::NodeContext;
use crate::TaskOutcome;
use crate::core::call_peer;
use crate::scheduler::spool::validate_slice_entry;

const SYNC_BATCH_SIZE: u32 = 100;

enum SyncSource {
    VerifyLocal,
    SyncFrom { node_id: NodeId },
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
    cancel: CancellationToken,
) -> TaskOutcome {

    let state = match ctx.store.get_spool_state(spool) {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::warn!(spool, "received spool sync task for spool with no state, skipping");
            return TaskOutcome::Success
        },
        Err(e) => {
            tracing::warn!(spool, "get_spool_state: {e}");
            return TaskOutcome::Retryable(format!("get_spool_state: {e}"))
        },
    };

    let (epoch, prev_owner, prev_helpers) = match state {
        SpoolState::Sync { epoch, prev_owner, prev_helpers } => (epoch, prev_owner, prev_helpers),
        _ => {
            tracing::warn!(spool, ?state, "received spool sync task for non-syncing spool, skipping");
            return TaskOutcome::Success;
        },
    };

    let source = match prev_owner {
        None => SyncSource::VerifyLocal,
        Some(id) if id == ctx.node_id() => SyncSource::VerifyLocal,
        Some(id) => SyncSource::SyncFrom { node_id: id },
    };

    match source {

        // If the task is running on the same node that previously owned the spool, we can skip
        // the sync loop and directly transition to Scan. The scan task will find any missing
        // slices and either transition to Active or Recover depending on gaps.
        SyncSource::VerifyLocal => {
            // no-op, just transition to Scan to trigger local verification in the next step.
        }

        // If the previous owner is a different node, we need to sync data from that peer. This is
        // the common case for normal spool handovers. We enter a sync loop where we fetch batches
        // of slices from the peer until we reach the end of the spool. After each batch, we
        // persist a cursor so that if the task is interrupted, we can resume from where we left
        // off.
        SyncSource::SyncFrom { node_id: peer } => {

            let mut cursor = match ctx.store.get_spool_sync_cursor(spool) {
                Ok(Some(val)) => Some(val.0),
                Ok(None) => None,
                Err(e) => return TaskOutcome::Retryable(format!("get_spool_sync_cursor: {e}")),
            };

            let mut total_bytes: u64 = 0;

            loop {
                if cancel.is_cancelled() {
                    return TaskOutcome::Success;
                }

                let req = SyncReq {
                    cursor,
                    spool_index: spool,
                    limit: SYNC_BATCH_SIZE,
                };

                let res = call_peer(&ctx.peer_manager, peer, Some(&cancel), || {
                    let api = ctx.api.clone();
                    let req = req.clone();
                    async move { api.sync(peer, &req).await }
                }).await;

                let res = match res {
                    Ok(res) => res,
                    Err(e) if e.is_retryable() || is_transient_error(&e) => {
                        tracing::warn!(spool, "failed api call for peer sync {}, retryable: {e}", peer.0);
                        return TaskOutcome::Retryable(format!("sync peer {}, retryable: {e}", peer.0));
                    }
                    Err(e) => {
                        tracing::warn!(spool, "failed api call for peer sync {}, permanent: {e}", peer.0);
                        return TaskOutcome::Permanent(format!("sync peer {}, permanent: {e}", peer.0));
                    }
                };

                if res.entries.is_empty() && res.next_cursor.is_none() {
                    break;
                }

                let mut last_track: Option<StorePubkey> = None;

                for entry in &res.entries {
                    let track_addr = StorePubkey(entry.track_address);
                    last_track = Some(track_addr);

                    // Skip if we already have this slice.
                    match ctx.store.has_slice(spool, track_addr) {
                        Ok(true) => {
                            continue;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "has_slice: {e}");
                            continue;
                        }
                    }

                    // Validate if track metadata exists.
                    match ctx.store.get_track(track_addr) {
                        Ok(Some(track_info)) => {
                            if let Err(reason) =
                                validate_slice_entry(spool, &track_info, &entry.slice_data)
                            {
                                tracing::warn!(?track_addr, spool, "skipping invalid slice: {reason}");
                                continue;
                            }
                        }
                        Ok(None) => {
                            // No track metadata yet, accept the slice for now.
                        }
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "get_track: {e}");
                            continue;
                        }
                    }

                    // Persist slice.
                    match ctx.store.put_slice(spool, track_addr, entry.slice_data.clone()) {
                        Ok(()) => {
                            total_bytes += entry.slice_data.len() as u64;
                        }
                        Err(e) => {
                            tracing::warn!(?track_addr, spool, "put_slice: {e}");
                        }
                    }
                }

                // Persist cursor after each batch.
                if let Some(track) = last_track {
                    if let Err(e) = ctx.store.set_spool_sync_cursor(spool, track) {
                        tracing::warn!(spool, "set_spool_sync_cursor: {e}");
                    }
                }

                match res.next_cursor {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }

            if total_bytes > 0 {
                ctx.stats.add_sync_received(total_bytes);
            }
        }
    }

    // After successfully syncing (or if no sync was needed), transition to Scan to find
    // any missing slices before potentially entering recovery or going active.

    let new_state = SpoolState::Scan {
        epoch, prev_owner, prev_helpers
    };

    if let Err(e) = ctx.store.set_spool_state(spool, new_state) {
        return TaskOutcome::Retryable(format!("set_spool_state: {e}"));
    }

    if let Err(e) = ctx.store.remove_spool_sync_cursor(spool) {
        return TaskOutcome::Retryable(format!("remove_spool_sync_cursor: {e}"));
    }

    TaskOutcome::Success
}

fn is_transient_error(err: &ApiError) -> bool {
    matches!(err, ApiError::NotResponsible | ApiError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    use peer_memory::MemoryApi;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::EpochNumber;
    use tape_protocol::api::{PeerReq, PeerRes, SyncRes, SyncSpoolEntry};
    use tape_store::ops::TrackOps;
    use tape_store::types::TrackInfo;

    use crate::core::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;
    const PEER: NodeId = NodeId(99);

    fn sync_state(epoch: EpochNumber, prev_owner: Option<NodeId>) -> SpoolState {
        SpoolState::Sync {
            epoch,
            prev_owner,
            prev_helpers: [None; SPOOL_GROUP_SIZE],
        }
    }

    fn active_state(epoch: EpochNumber) -> SpoolState {
        SpoolState::Active { epoch }
    }

    fn track_addr(n: u8) -> StorePubkey {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        StorePubkey(bytes)
    }

    fn entry(addr: StorePubkey, data: &[u8]) -> SyncSpoolEntry {
        SyncSpoolEntry {
            track_address: addr.0,
            slice_data: data.to_vec(),
        }
    }

    /// Build a TrackInfo whose spool_group maps to SPOOL.
    fn track_info_for_spool() -> TrackInfo {
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

    // early-exit tests 

    #[tokio::test]
    async fn sync_self_owner() {
        let ctx = test_context();
        let node_id = ctx.node_id();

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(node_id)))
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), SPOOL, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert!(matches!(
            state,
            SpoolState::Scan {
                epoch,
                prev_owner: Some(owner),
                ..
            } if epoch == EpochNumber(3) && owner == node_id
        ));
    }

    #[tokio::test]
    async fn sync_no_prev_owner() {
        let ctx = test_context();

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), None))
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), SPOOL, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert!(matches!(state, SpoolState::Scan { epoch, prev_owner: None, .. } if epoch == EpochNumber(3)));
    }

    #[tokio::test]
    async fn sync_not_syncing() {
        let ctx = test_context();

        ctx.store
            .set_spool_state(SPOOL, active_state(EpochNumber(3)))
            .unwrap();

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), SPOOL, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert!(matches!(state, SpoolState::Active { epoch } if epoch == EpochNumber(3)));
    }

    #[tokio::test]
    async fn sync_no_state() {
        let ctx = test_context();

        let cancel = CancellationToken::new();
        let result = run(ctx, SPOOL, cancel).await;
        assert!(matches!(result, TaskOutcome::Success));
    }

    #[tokio::test]
    async fn sync_unhealthy_peer() {
        let ctx = test_context();

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        for _ in 0..10 {
            ctx.peer_manager.report_failure(PEER);
        }

        let cancel = CancellationToken::new();
        let result = run(ctx.clone(), SPOOL, cancel).await;

        assert!(matches!(
            result,
            TaskOutcome::Retryable(_) | TaskOutcome::Permanent(_)
        ));
    }

    // sync loop tests

    #[tokio::test]
    async fn sync_one_batch() {
        let addr = track_addr(1);
        let data = vec![0xAB; 64];

        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                    entries: vec![entry(addr, &data)],
                    next_cursor: None,
                })),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Slice was persisted.
        assert!(ctx.store.has_slice(SPOOL, addr).unwrap());

        // State transitioned to Scan.
        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert!(matches!(state, SpoolState::Scan { epoch, .. } if epoch == EpochNumber(3)));

        // Cursor was cleaned up.
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());

        // Stats recorded.
        assert_eq!(ctx.stats.sync_bytes_received.load(Ordering::Relaxed), 64);
    }

    #[tokio::test]
    async fn sync_multi_batch() {
        let addr1 = track_addr(1);
        let addr2 = track_addr(2);
        let call_count = Arc::new(AtomicUsize::new(0));
        let counter = call_count.clone();

        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        PeerRes::Sync(Ok(SyncRes {
                            entries: vec![entry(addr1, &[1; 32])],
                            next_cursor: Some(addr1.0),
                        }))
                    } else {
                        PeerRes::Sync(Ok(SyncRes {
                            entries: vec![entry(addr2, &[2; 32])],
                            next_cursor: None,
                        }))
                    }
                }
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        assert!(ctx.store.has_slice(SPOOL, addr1).unwrap());
        assert!(ctx.store.has_slice(SPOOL, addr2).unwrap());
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(ctx.stats.sync_bytes_received.load(Ordering::Relaxed), 64);
    }

    #[tokio::test]
    async fn sync_resume_cursor() {
        let existing = track_addr(1);
        let addr2 = track_addr(2);
        let captured_cursor = Arc::new(std::sync::Mutex::new(None));
        let cursor_ref = captured_cursor.clone();

        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(ref r) => {
                    *cursor_ref.lock().unwrap() = r.cursor;
                    PeerRes::Sync(Ok(SyncRes {
                        entries: vec![entry(addr2, &[2; 16])],
                        next_cursor: None,
                    }))
                }
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.set_spool_sync_cursor(SPOOL, existing).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Verify the request carried the pre-existing cursor.
        assert_eq!(captured_cursor.lock().unwrap().unwrap(), existing.0);
    }

    #[tokio::test]
    async fn sync_skip_existing() {
        let addr = track_addr(1);
        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                    entries: vec![entry(addr, &[0xCC; 32])],
                    next_cursor: None,
                })),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        // Pre-populate the slice so has_slice returns true.
        ctx.store.put_slice(SPOOL, addr, vec![0xDD; 32]).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Original data preserved, not overwritten.
        let stored = ctx.store.get_slice(SPOOL, addr).unwrap().unwrap();
        assert_eq!(stored, vec![0xDD; 32]);

        // No bytes counted (skip path).
        assert_eq!(ctx.stats.sync_bytes_received.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sync_skip_invalid() {
        let addr = track_addr(1);
        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                    entries: vec![entry(addr, &[])], // empty data for non-empty track
                    next_cursor: None,
                })),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        // Store track metadata so validation runs — empty slice for a non-empty
        // track triggers the "empty slice for non-empty track" rejection.
        ctx.store.put_track(addr, track_info_for_spool()).unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        // Slice was NOT persisted.
        assert!(!ctx.store.has_slice(SPOOL, addr).unwrap());
    }

    #[tokio::test]
    async fn sync_accept_unknown_track() {
        let addr = track_addr(1);
        let data = vec![0xFF; 48];
        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                    entries: vec![entry(addr, &data)],
                    next_cursor: None,
                })),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        // No track metadata stored — should accept on trust.

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        assert!(ctx.store.has_slice(SPOOL, addr).unwrap());
    }


    #[tokio::test]
    async fn sync_retryable_error() {
        let ctx = test_context_with_api(
            MemoryApi::new(|_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Err(ApiError::Timeout)),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx, SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn sync_transient_error() {
        let ctx = test_context_with_api(
            MemoryApi::new(|_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Err(ApiError::NotResponsible)),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx, SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Retryable(_)));
    }

    #[tokio::test]
    async fn sync_permanent_error() {
        let ctx = test_context_with_api(
            MemoryApi::new(|_, req| match req {
                PeerReq::Sync(_) => PeerRes::Sync(Err(ApiError::Serialization("bad".into()))),
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx, SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Permanent(_)));
    }

    #[tokio::test]
    async fn sync_cursor_persisted() {
        let addr1 = track_addr(1);
        let addr2 = track_addr(2);
        let call_count = Arc::new(AtomicUsize::new(0));
        let counter = call_count.clone();

        let ctx = test_context_with_api(
            MemoryApi::new(move |_, req| match req {
                PeerReq::Sync(_) => {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        PeerRes::Sync(Ok(SyncRes {
                            entries: vec![entry(addr1, &[1; 8]), entry(addr2, &[2; 8])],
                            next_cursor: Some(addr2.0),
                        }))
                    } else {
                        PeerRes::Sync(Ok(SyncRes {
                            entries: vec![],
                            next_cursor: None,
                        }))
                    }
                }
                _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), SPOOL, CancellationToken::new()).await;
        assert!(matches!(result, TaskOutcome::Success));

        // After successful completion the cursor is cleaned up, but the slices
        // prove that the batch was processed. The cursor is only visible
        // mid-sync (between batches). Since we completed, it should be removed.
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());
    }
}
