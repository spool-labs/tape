use std::sync::Arc;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::leaf_position;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps, track_has_left};
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
// 1. Sweep all tracks via store.sweep_tracks(mark, batch_size):
//    a. Check cancellation.
//    b. For each (track_address, track_info) in the batch:
//       - Skip if the track's group doesn't fold to this live spool's group.
//       - Check if we have the slice locally via has_slice, under the
//         track's own stored spool.
//       - If missing → add_pending_repair(stored spool, track_address).
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
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> ScanResult {

    let mut cursor: Option<Vec<u8>> = None;
    let mut gaps = 0usize;
    let mut had_error = false;

    // The spool is a live one. A track belongs to it when its group folds to
    // the spool's group, and its slice is kept under the track's own group.
    let Some(fold) = ctx.state().fold_at(epoch) else {
        warn!(spool = %spool, epoch = epoch.0, "scan: no fold for the epoch");
        return ScanResult::Retry;
    };
    let group = GroupIndex::containing(spool);
    let position = leaf_position(spool).as_usize();
    let batch_size = config.scan_batch.max(1);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let (tracks, next) = match ctx.store.sweep_tracks(cursor.as_deref(), batch_size) {
            Ok(swept) => swept,
            Err(error) => {
                warn!(spool = %spool, %error, "scan sweep_tracks failed");
                had_error = true;
                break;
            }
        };

        let mut in_group: Vec<(Address, &CompressedTrack)> = Vec::with_capacity(tracks.len());
        for (track_addr, track_info) in &tracks {
            // Tracks outside this spool's group belong to another node, and a raw
            // track has no slice semantics to repair.
            if fold.live_group(track_info.group) == group && track_info.is_coded() {
                in_group.push((*track_addr, track_info));
            }
        }

        // A track that has left its tape is deleted by the sweep, so no gap of
        // its is repaired. The batch's tape rows come in one call.
        let parents: Vec<Address> = in_group.iter().map(|(_, track_info)| track_info.tape).collect();
        let tapes = match ctx.store.get_tapes(&parents) {
            Ok(tapes) => tapes,
            Err(error) => {
                warn!(spool = %spool, %error, "scan get_tapes failed");
                had_error = true;
                break;
            }
        };

        let mut considered: Vec<Address> = Vec::with_capacity(in_group.len());
        let mut stored_spools: Vec<SpoolIndex> = Vec::with_capacity(in_group.len());
        for ((track_addr, track_info), tape) in in_group.into_iter().zip(tapes) {
            if !track_has_left(tape.as_ref(), track_info) {
                considered.push(track_addr);
                stored_spools.push(track_info.group.spool_at(position));
            }
        }

        // The batch's certify status in one call rather than one per track, since
        // a decision per row is what makes a sweep a round trip per row.
        let infos = match ctx.store.get_object_infos(&considered) {
            Ok(infos) => infos,
            Err(error) => {
                warn!(spool = %spool, %error, "scan get_object_infos failed");
                had_error = true;
                break;
            }
        };

        for ((track_addr, stored), info) in considered.iter().zip(stored_spools).zip(infos) {
            // Only consider certified tracks for repair
            match info {
                Some(info) if info.is_certified() => {}
                Some(_) => continue,
                None => {
                    warn!(spool = %spool, track = %track_addr, "scan: track exists but ObjectInfo missing");
                    had_error = true;
                    continue;
                }
            }

            // Check if slice exists locally. A containment check reads the index
            // and never the payload, so there is nothing here for a batch to win.
            let has_slice = match ctx.store.has_slice(stored, *track_addr) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool = %stored, track = %track_addr, %error, "scan has_slice failed");
                    had_error = true;
                    continue;
                }
            };

            if has_slice {
                continue;
            }

            if let Err(error) = ctx.store.add_pending_repair(stored, *track_addr) {
                warn!(spool = %stored, track = %track_addr, %error, "scan add_pending_repair failed");
                had_error = true;
                continue;
            }

            gaps += 1;
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
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
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{SlotNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_store::ops::ObjectInfoOps;
    use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};

    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    const TAPE_NUMBER: TapeNumber = TapeNumber(1);

    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

    // a node that holds the row of the tape every test track is on
    async fn test_context() -> TestContext {
        context_with_groups(1).await
    }

    async fn context_with_groups(live_groups: u64) -> TestContext {
        let ctx = NodeHarness::builder()
            .nodes(live_groups as usize * WIDTH + 5)
            .current_group_count(live_groups)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize());

        let tape = TapeInfo::new(TAPE_NUMBER, TapeFlags::NONE, EpochNumber(u64::MAX), TrackNumber(1));
        ctx.store.put_tape(addr(0), tape).expect("put tape");
        ctx
    }

    async fn scan(ctx: &TestContext) -> ScanResult {
        let epoch = ctx.state().epoch();
        run(ctx.clone(), &RecoveryConfig::default(), SPOOL, epoch, &CancellationToken::new()).await
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn track(group: VirtualGroup) -> CompressedTrack {
        CompressedTrack {
            tape: addr(0),
            tape_number: TAPE_NUMBER,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group,
            value_hash: Hash::new_unique(),
        }
    }

    #[tokio::test]
    async fn no_tracks() {
        let ctx = test_context().await;
        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    #[tokio::test]
    async fn all_present() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = VirtualGroup::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();

        let result = scan(&ctx).await;
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
        let group = VirtualGroup::containing(SPOOL);

        // Certified track exists but no slice data.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });

        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    // with two live groups, a track of the other one is another node's to hold
    #[tokio::test]
    async fn skips_other_groups() {
        let ctx = context_with_groups(2).await;
        let a = addr(1);
        let other_group = VirtualGroup(VirtualGroup::containing(SPOOL).0 + 1);

        ctx.store.put_track(a, track(other_group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
    }

    // a gap in a group the live spool carries is queued under that group's stored spool
    #[tokio::test]
    async fn carried_group_gap() {
        let ctx = test_context().await;
        let a = addr(1);
        let carried = VirtualGroup(VirtualGroup::containing(SPOOL).0 + 1);
        let stored = carried.spool_at(SPOOL.as_usize());

        ctx.store.put_track(a, track(carried)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });

        assert!(ctx.store.has_pending_repair(stored, a).unwrap());
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn idempotent_adds() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = VirtualGroup::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        // Run scan twice, same result, no duplicates.
        let r1 = scan(&ctx).await;
        let r2 = scan(&ctx).await;

        assert_eq!(r1, ScanResult::Done { gaps: 1 });
        assert_eq!(r2, ScanResult::Done { gaps: 1 });
    }

    #[tokio::test]
    async fn skips_uncertified() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = VirtualGroup::containing(SPOOL);

        // Track exists, no slice, but NOT certified.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, ObjectInfo::Valid {
            track_address: a,
            registered_epoch: EpochNumber(1),
            certified_epoch: None,
            slot: SlotNumber(10),
        }).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn scans_certified() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = VirtualGroup::containing(SPOOL);

        // Track exists, no slice, IS certified -> should be a gap.
        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });
        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn scans_snapshot_tracks() {
        let ctx = test_context().await;
        let a = addr(1);
        let group = VirtualGroup::containing(SPOOL);

        ctx.store.put_track(a, track(group)).unwrap();
        ctx.store.put_object_info(a, snapshot(a)).unwrap();

        let result = scan(&ctx).await;
        assert_eq!(result, ScanResult::Done { gaps: 1 });
        assert!(ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }
}
