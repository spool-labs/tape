use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_protocol::Api;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::features::spool::types::ScanResult;

// Purpose: Audit local storage to find missing slices that need repair.
//          Adds to the pending_repairs queue for the Repair task.
//
// Scan is local-only (no remote calls) and fast. No cursor needed,
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
// deleted) are harmless, repair skips and removes them.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    cancel: &CancellationToken,
) -> ScanResult {

    let mut cursor = None;
    let mut gaps = 0usize;
    let mut had_error = false;

    let group = GroupIndex::containing(spool);
    let batch_size = config.scan_batch.max(1);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let tracks = match ctx
            .store
            .iter_tracks_from(cursor, batch_size)
        {
            Ok(tracks) => tracks,
            Err(error) => {
                warn!(spool = %spool, %error, "scan iter_tracks_from failed");
                had_error = true;
                break;
            }
        };

        if tracks.is_empty() {
            break;
        }

        for (track_addr, track_info) in &tracks {
            // Skip tracks not in this spool's group.
            if track_info.group != group {
                continue;
            }

            // Raw tracks have no slice semantics and should never enter repair.
            if !track_info.is_blob() {
                continue;
            }

            // Only consider certified tracks for repair
            match ctx.store.get_object_info(*track_addr) {
                Ok(Some(info)) if info.is_certified() => {}
                Ok(Some(_)) => continue,
                Ok(None) => {
                    warn!(spool = %spool, track = %track_addr, "scan: track exists but ObjectInfo missing");
                    had_error = true;
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "scan get_object_info failed");
                    had_error = true;
                    continue;
                }
            }

            // Check if slice exists locally.
            let has_slice = match ctx.store.has_slice(spool, *track_addr) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "scan has_slice failed");
                    had_error = true;
                    continue;
                }
            };

            if has_slice {
                continue;
            }

            if let Err(error) = ctx.store.add_pending_repair(spool, *track_addr) {
                warn!(spool = %spool, track = %track_addr, %error, "scan add_pending_repair failed");
                had_error = true;
                continue;
            }

            gaps += 1;
        }

        cursor = tracks
            .last()
            .map(|(track_addr, _)| *track_addr);
    }

    if had_error {
        ScanResult::Retry
    } else {
        ScanResult::Done { gaps }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_store::ops::ObjectInfoOps;
    use tape_store::types::{ObjectInfo, SystemObjectKind};

    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn track(group: GroupIndex) -> CompressedTrack {
        let _profile = EncodingProfile::clay_default();
        CompressedTrack {
            tape: Address::from([0; 32]),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(1024),
            group: group,
            value_hash: Hash::new_unique(),
        }
    }

    #[tokio::test]
    async fn no_tracks() {
        let ctx = test_context().await;
        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn all_present() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    fn certified(track_address: Address) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address,
            registered_epoch: EpochNumber(1),
            certified_epoch: Some(EpochNumber(2)),
            slot: SlotNumber(10),
        }
    }

    fn snapshot(track_address: Address) -> ObjectInfo {
        ObjectInfo::System {
            kind: SystemObjectKind::Snapshot {
                epoch: EpochNumber(2),
            },
            track_address,
            registered_epoch: EpochNumber(2),
            certified_epoch: Some(EpochNumber(2)),
            slot: SlotNumber(10),
        }
    }

    #[tokio::test]
    async fn finds_gaps() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        // Certified track exists but no slice data.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });

        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn skips_other_groups() {
        let ctx = test_context().await;
        let a = addr(1);
        let other_group = GroupIndex::containing(SPOOL + SpoolIndex(20)); // Different group.

        ctx.store.put_track(a, track(other_group)).unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn idempotent_adds() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        // Run scan twice, same result, no duplicates.
        let r1 = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        let r2 = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(r1, ScanResult::Done { gaps: 1 });
        assert_eq!(r2, ScanResult::Done { gaps: 1 });
    }

    #[tokio::test]
    async fn skips_uncertified() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        // Track exists, no slice, but NOT certified.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, ObjectInfo::Valid {
            track_address: a,
            registered_epoch: EpochNumber(1),
            certified_epoch: None,
            slot: SlotNumber(10),
        }).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn scans_certified() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        // Track exists, no slice, IS certified -> should be a gap.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });
        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn scans_snapshot_tracks() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = GroupIndex::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, snapshot(a)).unwrap();

        let result = run(
            ctx.clone(),
            &RecoveryConfig::default(),
            SPOOL,
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });
        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }
}
