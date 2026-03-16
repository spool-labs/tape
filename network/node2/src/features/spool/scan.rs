use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::features::spool::types::ScanResult;

// Purpose: Audit local storage to find missing slices that need repair.
//          Adds to the pending_repairs queue for the Repair task.
//
// Scan is local-only (no remote calls) and fast. No cursor needed —
// if interrupted, the next scan restarts from the beginning.
// Adds are idempotent (presence-based queue), so re-scanning is safe.
//
// Algorithm:
// 1. Paginate over all tracks via store.iter_tracks_from(cursor, batch_size):
//    a. Check cancellation.
//    b. For each (track_address, track_info) in the batch:
//       - Skip if track's spool group doesn't include this spool.
//       - Check if we have the slice locally via has_slice.
//       - If missing → add_pending_repair(spool, track_address).
//         Increment gap counter.
//    c. Advance cursor to last track in the batch.
//    d. Stop when batch is empty.
// 2. Return Done { gaps }.
//
// Stale entries in pending_repairs (slice already obtained, or track
// deleted) are harmless — repair skips and removes them.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    cancel: &CancellationToken,
) -> ScanResult {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::types::EpochNumber;
    use tape_store::types::{SpoolState, SpoolStatus, TrackInfo};

    use crate::core::context::test_utils::test_context;

    const SPOOL: SpoolIndex = 5;

    fn addr(n: u8) -> Pubkey {
        Pubkey([n; 32])
    }

    fn track(group: SpoolGroup) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group: group,
            original_size: 1024,
            stripe_size: 512,
            stripe_count: 2,
            encoding_type: EncodingProfile::clay_default().encoding as u64,
            encoding_params: EncodingProfile::clay_default().params,
            commitment: vec![],
        }
    }

    #[tokio::test]
    async fn no_tracks() {
        let ctx = test_context();
        let result = run(ctx, &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn all_present() {
        let ctx = test_context();
        let a = addr(1);
        let group = SpoolGroup::of(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();

        let result = run(ctx, &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn finds_gaps() {
        let ctx = test_context();
        let a = addr(1);
        let group = SpoolGroup::of(SPOOL);

        // Track exists but no slice data.
        ctx.store.put_track(a, track(group)).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });

        // Track should be in pending_repairs queue.
        // assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn skips_other_groups() {
        let ctx = test_context();
        let a = addr(1);
        let other_group = SpoolGroup::of(SPOOL + 20); // Different group.

        ctx.store.put_track(a, track(other_group)).unwrap();

        let result = run(ctx, &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn idempotent_adds() {
        let ctx = test_context();
        let a = addr(1);
        let group = SpoolGroup::of(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();

        // Run scan twice — same result, no duplicates.
        let r1 = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        let r2 = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(r1, ScanResult::Done { gaps: 1 });
        assert_eq!(r2, ScanResult::Done { gaps: 1 });
    }
}
