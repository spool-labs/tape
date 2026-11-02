use std::sync::Arc;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_store::TapeStore;
use tape_store::ops::{
    ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps, track_has_left,
};
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
// 1. Sweep the store for the live spool's coded tracks with no local slice,
//    each under its track's own stored spool (sweep_gaps). Sync builds its
//    missing list from the same sweep.
// 2. Queue each certified gap with add_pending_repair.
// 3. Return Done { gaps }, or Retry when a read failed.
//
// Stale entries in pending_repairs (slice already obtained, or track
// deleted) are harmless, repair skips and removes them.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    pub track: Address,
    pub stored: SpoolIndex,
    pub is_certified: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineGap {
    pub track: Address,
    pub group: VirtualGroup,
    pub value_hash: Hash,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Gaps {
    pub coded: Vec<Gap>,
    pub inline: Vec<InlineGap>,
    pub has_error: bool,
}

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> ScanResult {

    // The spool is a live one. A track belongs to it when its group folds to
    // the spool's group, and its slice is kept under the track's own group.
    let Some(fold) = ctx.state().fold_at(epoch) else {
        warn!(spool = %spool, epoch = epoch.0, "scan: no fold for the epoch");
        return ScanResult::Retry;
    };

    let found = sweep_gaps(ctx.store.as_ref(), fold, spool, config.scan_batch.max(1), false, cancel);
    let mut has_error = found.has_error;
    let mut gaps = 0usize;

    // Only certified tracks are queued for repair
    for gap in found.coded.into_iter().filter(|gap| gap.is_certified) {
        if let Err(error) = ctx.store.add_pending_repair(gap.stored, gap.track) {
            warn!(spool = %gap.stored, track = %gap.track, %error, "scan add_pending_repair failed");
            has_error = true;
            continue;
        }

        gaps += 1;
    }

    if has_error {
        ScanResult::Retry
    } else {
        ScanResult::Done { gaps }
    }
}

/// Sweep the store for the live spool's tracks this node holds nothing of
///
/// A track that has left its tape has no gap. A failed read for a batch ends
/// the sweep and one for a single track skips it, and both flag the result.
pub fn sweep_gaps<Db: Store>(
    store: &TapeStore<Db>,
    fold: GroupFold,
    spool: SpoolIndex,
    batch_size: usize,
    should_find_inline: bool,
    cancel: &CancellationToken,
) -> Gaps {
    let group = GroupIndex::containing(spool);
    let position = leaf_position(spool).as_usize();
    let mut gaps = Gaps::default();
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let (tracks, next) = match store.sweep_tracks(cursor.as_deref(), batch_size) {
            Ok(swept) => swept,
            Err(error) => {
                warn!(spool = %spool, %error, "sweep_tracks failed");
                gaps.has_error = true;
                break;
            }
        };

        // Tracks outside this spool's group belong to another node.
        let mut in_group: Vec<(Address, &CompressedTrack)> = Vec::with_capacity(tracks.len());
        for (track_addr, track_info) in &tracks {
            let is_wanted = track_info.is_coded() || (should_find_inline && track_info.is_inline());
            if fold.live_group(track_info.group) == group && is_wanted {
                in_group.push((*track_addr, track_info));
            }
        }

        // A track that has left its tape is deleted by the sweep, so it has no
        // gap. The batch's tape rows come in one call.
        let parents: Vec<Address> = in_group.iter().map(|(_, track_info)| track_info.tape).collect();
        let tapes = match store.get_tapes(&parents) {
            Ok(tapes) => tapes,
            Err(error) => {
                warn!(spool = %spool, %error, "sweep get_tapes failed");
                gaps.has_error = true;
                break;
            }
        };

        let mut coded: Vec<(Address, SpoolIndex)> = Vec::with_capacity(in_group.len());
        for ((track_addr, track_info), tape) in in_group.into_iter().zip(tapes) {
            if track_has_left(tape.as_ref(), track_info) {
                continue;
            }

            if track_info.is_coded() {
                coded.push((track_addr, track_info.group.spool_at(position)));
                continue;
            }

            match store.has_track_data(track_addr) {
                Ok(true) => {}
                Ok(false) => gaps.inline.push(InlineGap {
                    track: track_addr,
                    group: track_info.group,
                    value_hash: track_info.value_hash,
                }),
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "sweep has_track_data failed");
                    gaps.has_error = true;
                }
            }
        }

        // The batch's certify status in one call rather than one per track, since
        // a decision per row is what makes a sweep a round trip per row.
        let addresses: Vec<Address> = coded.iter().map(|(track_addr, _)| *track_addr).collect();
        let infos = match store.get_object_infos(&addresses) {
            Ok(infos) => infos,
            Err(error) => {
                warn!(spool = %spool, %error, "sweep get_object_infos failed");
                gaps.has_error = true;
                break;
            }
        };

        for ((track_addr, stored), info) in coded.into_iter().zip(infos) {
            let Some(info) = info else {
                warn!(spool = %spool, track = %track_addr, "sweep: track exists but ObjectInfo missing");
                gaps.has_error = true;
                continue;
            };

            // A containment check reads the index and never the payload, so there
            // is nothing here for a batch to win.
            match store.has_slice(stored, track_addr) {
                Ok(true) => {}
                Ok(false) => gaps.coded.push(Gap {
                    track: track_addr,
                    stored,
                    is_certified: info.is_certified(),
                }),
                Err(error) => {
                    warn!(spool = %stored, track = %track_addr, %error, "sweep has_slice failed");
                    gaps.has_error = true;
                }
            }
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::data::BlobData;
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

    fn sweep(ctx: &TestContext, should_find_inline: bool) -> Gaps {
        let fold = ctx.state().fold_at(ctx.state().epoch()).expect("fold");
        sweep_gaps(ctx.store.as_ref(), fold, SPOOL, 100, should_find_inline, &CancellationToken::new())
    }

    fn inline_track(group: VirtualGroup) -> CompressedTrack {
        CompressedTrack {
            kind: TrackKind::Inline as u64,
            ..track(group)
        }
    }

    // the sweep reports an uncertified gap too, flagged, so sync can ask for it
    #[tokio::test]
    async fn sweep_flags_uncertified() {
        let ctx = test_context().await;
        let group = VirtualGroup::containing(SPOOL);
        let certified_track = addr(1);
        let registered_track = addr(2);

        ctx.store.put_track(certified_track, track(group)).unwrap();
        ctx.store.put_object_info(certified_track, certified(certified_track)).unwrap();
        ctx.store.put_track(registered_track, track(group)).unwrap();
        ctx.store.put_object_info(registered_track, ObjectInfo::Valid {
            track_address: registered_track,
            registered_epoch: EpochNumber(1),
            certified_epoch: None,
            slot: SlotNumber(10),
        }).unwrap();

        let mut gaps = sweep(&ctx, false);
        gaps.coded.sort_by_key(|gap| gap.track);
        assert_eq!(gaps.coded, vec![
            Gap { track: certified_track, stored: SPOOL, is_certified: true },
            Gap { track: registered_track, stored: SPOOL, is_certified: false },
        ]);
        assert!(!gaps.has_error);
    }

    // inline tracks with no payload come back only when the sweep asks for them
    #[tokio::test]
    async fn sweep_finds_inline() {
        let ctx = test_context().await;
        let group = VirtualGroup::containing(SPOOL);
        let missing = addr(1);
        let held = addr(2);

        ctx.store.put_track(missing, inline_track(group)).unwrap();
        ctx.store.put_track(held, inline_track(group)).unwrap();
        ctx.store.put_track_data(held, BlobData::Inline(vec![7; 64])).unwrap();

        assert!(sweep(&ctx, false).inline.is_empty());

        let gaps = sweep(&ctx, true);
        let inline: Vec<Address> = gaps.inline.iter().map(|gap| gap.track).collect();
        assert_eq!(inline, vec![missing]);
        assert!(gaps.coded.is_empty());
    }

    // a track that has left its tape has no gap
    #[tokio::test]
    async fn sweep_skips_left_tracks() {
        let ctx = test_context().await;
        let group = VirtualGroup::containing(SPOOL);
        let a = addr(1);
        let b = addr(2);

        // No tape row sits at this address, so both tracks have left their tape.
        ctx.store.put_track(a, CompressedTrack { tape: addr(9), ..track(group) }).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.put_track(b, CompressedTrack { tape: addr(9), ..inline_track(group) }).unwrap();

        let gaps = sweep(&ctx, true);
        assert!(gaps.coded.is_empty());
        assert!(gaps.inline.is_empty());
        assert!(!gaps.has_error);
    }
}
