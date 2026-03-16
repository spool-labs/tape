use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::features::spool::types::SyncResult;

// Purpose: Transfer slice data for a spool from its previous owner
//          after a spool reassignment.
//
// Algorithm:
// 1. Load spool state from the store. If missing, return Done.
// 2. Determine source: if no previous owner, or we are the previous
//    owner, return Done (nothing to sync).
// 3. Paginated pull from the previous owner via call_peer + api.sync:
//    - Load the sync cursor (last track we left off at).
//    - Loop:
//      a. Check cancellation.
//      b. Send SyncReq to previous owner with cursor + batch limit.
//      c. For each entry in the response:
//         - Skip if we already have the slice locally (has_slice).
//         - If we have the track metadata, validate the slice against
//           the commitment. Skip invalid entries.
//         - put_slice to store.
//      d. Advance the cursor to the last track in the batch.
//         Persist cursor so we can resume if interrupted.
//      e. Stop when the peer returns no entries and no next cursor.
// 4. Clear the sync cursor. Return Done.
//
// If the previous owner is unreachable, return Unavailable.
// The FSM treats Unavailable the same as Done — it moves to Scan,
// which will identify the gaps, and repair/recover will fetch from
// the rest of the spool group.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    cancel: &CancellationToken,
) -> SyncResult {
    todo!()
}

/// Pull one page of slices from the previous owner, persist each valid entry.
/// Returns the next cursor, or None if the peer has no more data.
async fn pull_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    prev_owner: NodeId,
    cursor: Option<Pubkey>,
) -> Result<Option<Pubkey>, SyncError> {
    todo!()
}

enum SyncError {
    Unavailable,
    Store(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::types::EpochNumber;
    use tape_protocol::api::ops::{PeerReq, PeerRes, SyncRes};
    use tape_protocol::api::types::SyncSpoolEntry;
    use tape_store::types::{SpoolState, SpoolStatus};

    use crate::core::context::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;
    const PEER: NodeId = NodeId(99);

    fn addr(n: u8) -> Pubkey {
        Pubkey([n; 32])
    }

    fn entry(track: Pubkey, data: &[u8]) -> SyncSpoolEntry {
        SyncSpoolEntry {
            track_address: track.0,
            slice_data: data.to_vec(),
        }
    }

    fn sync_state(epoch: EpochNumber, prev: Option<NodeId>) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Sync, epoch);
        state.prev_owner = prev;
        state
    }

    #[tokio::test]
    async fn no_prev_owner() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), None))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Done);
    }

    #[tokio::test]
    async fn pulls_slices() {
        let a = addr(1);
        let data = vec![0xAB; 64];
        let data_clone = data.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &data_clone)],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Done);
        assert!(ctx.store.has_slice(SPOOL, a).unwrap());
        assert!(ctx.store.get_spool_sync_cursor(SPOOL).unwrap().is_none());
    }

    #[tokio::test]
    async fn skips_existing() {
        let a = addr(1);
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(_) => PeerRes::Sync(Ok(SyncRes {
                entries: vec![entry(a, &[0xAB; 64])],
                next_cursor: None,
            })),
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xFF; 32]).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Done);

        // Original data preserved, not overwritten.
        let stored = ctx.store.get_slice(SPOOL, a).unwrap().unwrap();
        assert_eq!(stored, vec![0xFF; 32]);
    }

    #[tokio::test]
    async fn peer_unavailable() {
        let ctx = test_context(); // noop api returns errors
        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Unavailable);
    }

    #[tokio::test]
    async fn resumes_cursor() {
        let a1 = addr(1);
        let a2 = addr(2);
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = call_count.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Sync(ref r) => {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n == 0 {
                    // First call: return a1 with cursor pointing to a2.
                    PeerRes::Sync(Ok(SyncRes {
                        entries: vec![entry(a1, &[1; 32])],
                        next_cursor: Some(a2.0),
                    }))
                } else {
                    // Second call: return a2, no more.
                    PeerRes::Sync(Ok(SyncRes {
                        entries: vec![entry(a2, &[2; 32])],
                        next_cursor: None,
                    }))
                }
            }
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, sync_state(EpochNumber(3), Some(PEER)))
            .unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, SyncResult::Done);
        assert!(ctx.store.has_slice(SPOOL, a1).unwrap());
        assert!(ctx.store.has_slice(SPOOL, a2).unwrap());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
