use std::sync::Arc;

use peer_manager::PeerManager;
use rpc::Rpc;
use store::Store;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_core::track::data::BlobData;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::ops::GetSliceReq;
use tape_retry::RetryConfig;
use tape_slicer::{ClayCoder, ErasureCoder, SliceIndex, SliceMetadata, Slicer};
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn, Instrument};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::repair::{GroupPeers, group_peers};
use crate::features::spool::types::RecoverResult;

const RECOVER_FETCH_CONCURRENCY: usize = 4;

// Purpose: Full erasure code recovery for slices that could not be Clay-repaired.
//          Drains the pending_recoveries queue populated by the Repair task.
//
// Algorithm:
// 1. Load spool state. Derive group and our slice index.
//    Build two peer maps (previous, current), same as repair.
//
// 2. Batch loop over store.iter_pending_recoveries(spool, batch_size):
//
//    For each track_address:
//      a. Check cancellation.
//      b. Skip if slice already present (has_slice). Remove from queue.
//      c. Load track_info. If missing, remove from queue, continue.
//
//      d. Fetch k full slices (per-track: per-helper fallback across both peer maps):
//         - For each helper position in the spool group (excluding ours):
//           try the previous peer map first, fall back to the current peer map.
//           Keep the first success per position. Accumulate across both sources.
//         - If total >= k valid slices → proceed. Otherwise track stays pending.
//
//      e. Reconstruct:
//         - ClayCoder::from_params(track_info.profile().clay_params())
//         - Slicer::with_profile(coder, stripe_size, rotated=true, profile)
//         - Parse SliceMetadata from any fetched slice to get chunk_index.
//         - slicer.set_chunk_index(metadata.chunk_index)
//         - decoded = slicer.decode(&slice_refs)
//         - reencoded = slicer.encode(&decoded)
//         - Extract reencoded[our_slice_index].
//
//      f. Validate against track_info.verify_slice(our_position, &data).
//         If invalid → leave pending, continue.
//
//      g. Persist: store.put_slice(spool, track_address, data).
//         Remove from pending_recoveries.
//
// 3. Count remaining. Return Done { remaining }.
//
// NOTE:
//
// The spool relationships are:
//   - GroupIndex::containing(spool) → group is derived from spool
//   - group.position_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the GroupIndex and the SliceIndex within it. So passing
//   spool, group, AND lost is redundant, any one of these plus spool is computable from the other.
//   The helpers should just take spool and derive what they need.

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> RecoverResult {

    let spool_state = match ctx.store.get_spool_state(spool) {
        Ok(Some(state)) => state,
        Ok(None) => return RecoverResult::Done { remaining: 0 },
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read spool state");
            return RecoverResult::Done { remaining: 0 };
        }
    };

    let peers = group_peers(ctx.as_ref(), &spool_state, spool);
    let group = GroupIndex::containing(spool);
    let position = group.position_of(spool).unwrap_or_default();
    let batch_size = config.recover_batch.max(1);

    loop {
        if token.is_cancelled() {
            break;
        }

        let pending = match ctx
            .store
            .iter_pending_recoveries(spool, batch_size)
        {
            Ok(pending) => pending,
            Err(error) => {
                warn!(spool = %spool, %error, "iter_pending_recoveries failed");
                break;
            }
        };

        if pending.is_empty() {
            break;
        }

        let mut made_progress = false;
        for track_addr in pending {
            if token.is_cancelled() {
                break;
            }

            let has_slice = match ctx.store.has_slice(spool, track_addr) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "has_slice failed");
                    continue;
                }
            };

            if has_slice {
                let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                made_progress = true;
                continue;
            }

            let track_info = match ctx.store.get_track(track_addr) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    made_progress = true;
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "get_track failed");
                    continue;
                }
            };

            if !track_info.is_coded() {
                warn!(spool = %spool, track = %track_addr, "non-blob track in recovery queue");
                continue;
            }

            let track_data = match ctx.store.get_track_data(track_addr) {
                Ok(Some(BlobData::Coded(info))) => info,
                Ok(Some(BlobData::Inline(_))) => {
                    warn!(spool = %spool, track = %track_addr, "blob track has raw track_data, keeping queued");
                    continue;
                }
                Ok(None) => {
                    warn!(spool = %spool, track = %track_addr, "track_data missing, keeping queued");
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "get_track_data failed");
                    continue;
                }
            };

            // Only consider certified tracks for recovery
            match ctx.store.get_object_info(track_addr) {
                Ok(Some(info)) if info.is_certified() => {}
                Ok(Some(_)) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    made_progress = true;
                    continue;
                }
                Ok(None) | Err(_) => {
                    warn!(spool = %spool, track = %track_addr, "recover: skipping, state inconsistent or unreadable");
                    continue;
                }
            }

            let profile = track_data.profile;
            if !profile.is_clay() || track_data.stripe_size == StorageUnits::zero() {
                continue;
            }

            let mut slicer = Slicer::with_profile(
                ClayCoder::from_params(profile.clay_params()),
                track_data.stripe_size.as_usize(),
                true,
                profile,
            );
            let k = slicer.k();

            let peer_slices = match fetch_slices(
                ctx.as_ref(), spool, k, &peers, track_addr, token
            ).await
            {
                Ok(peer_slices) => peer_slices,
                Err(()) => continue,
            };

            let recovered =
                match reconstruct(&mut slicer, SliceIndex::new(position as usize), &peer_slices) {
                    Ok(recovered) => recovered,
                    Err(error) => {
                        debug!(spool = %spool, track = %track_addr, %error, "reconstruct failed");
                        continue;
                    }
                };

            if !track_data.verify_slice(SpoolIndex::from(position as u64), &recovered) {
                continue;
            }

            let recovered_len = recovered.len() as u64;
            if let Err(error) = ctx.store.put_slice(spool, track_addr, recovered) {
                warn!(spool = %spool, track = %track_addr, %error, "put_slice failed");
                continue;
            }

            ctx.metrics.add_recover_persisted(recovered_len);
            let _ = ctx.store.remove_pending_recovery(spool, track_addr);

            made_progress = true;
        }

        if !made_progress {
            break;
        }
    }

    let remaining = ctx
        .store
        .iter_pending_recoveries(spool, usize::MAX)
        .map(|pending| pending.len())
        .unwrap_or(0);

    RecoverResult::Done { remaining }
}

/// Fetch one full slice from a helper position using per-helper fallback.
///
/// Tries the previous peer first, then the current.
/// Returns the fetched slice data on first non-empty success, or Err on failure.
async fn fetch_one_slice<Cluster: Api + 'static>(
    peer_manager: Arc<PeerManager>,
    api: Arc<Cluster>,
    token: CancellationToken,
    candidates: [Option<Address>; 2],
    request: GetSliceReq,
    helper_slice: usize,
) -> Result<(SliceIndex, Vec<u8>), ()> {
    for node_id in candidates.into_iter().flatten() {
        if let Ok(res) = call_peer(
            &peer_manager,
            RetryConfig::three(),
            node_id,
            Some(&token),
            || api.get_slice(node_id, &request),
        ).await {
            if !res.data.is_empty() {
                return Ok((SliceIndex::new(helper_slice), res.data));
            }
        }
    }
    Err(())
}

/// Fetch k full slices for a given track using bounded concurrency.
///
/// For each helper position, tries the previous peer map first, then the current.
/// Runs up to RECOVER_FETCH_CONCURRENCY helper-position fetches in parallel.
/// Returns collected (slice_index, data) pairs, or Err if < k available.
async fn fetch_slices<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    k: usize,
    peers: &GroupPeers,
    track_addr: Address,
    token: &CancellationToken,
) -> Result<Vec<(SliceIndex, Vec<u8>)>, ()> {

    let group = GroupIndex::containing(spool);
    let track = track_addr;
    let mut slices = Vec::with_capacity(k);

    let positions: Vec<usize> = (0..GROUP_SIZE)
        .filter(|&pos| group.spool_at(pos) != spool)
        .collect();
    let mut pos_iter = positions.into_iter();

    let mut join_set: JoinSet<Result<(SliceIndex, Vec<u8>), ()>> = JoinSet::new();

    // Seed initial batch.
    for _ in 0..RECOVER_FETCH_CONCURRENCY {
        if token.is_cancelled() {
            return Err(());
        }
        let Some(helper_slice) = pos_iter.next() else { break };
        let helper_spool = group.spool_at(helper_slice);
        let prev_id = peers.previous.get(&helper_spool).copied();
        let curr_id = peers.current.get(&helper_spool).copied();
        let candidates = [
            prev_id,
            curr_id.filter(|id| prev_id.map_or(true, |p| p != *id)),
        ];
        let request = GetSliceReq { track, spool: helper_spool };
        join_set.spawn(
            fetch_one_slice(
                ctx.peer_manager.clone(),
                ctx.api.clone(),
                token.clone(),
                candidates,
                request,
                helper_slice,
            )
            .in_current_span(),
        );
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok((idx, data))) => {
                ctx.metrics.add_recover_fetched(data.len() as u64);
                slices.push((idx, data));

                if slices.len() >= k {
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    return Ok(slices);
                }

                if token.is_cancelled() {
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    return Err(());
                }

                if let Some(next_pos) = pos_iter.next() {
                    let helper_spool = group.spool_at(next_pos);
                    let prev_id = peers.previous.get(&helper_spool).copied();
                    let curr_id = peers.current.get(&helper_spool).copied();
                    let candidates = [
                        prev_id,
                        curr_id.filter(|id| prev_id.map_or(true, |p| p != *id)),
                    ];
                    let request = GetSliceReq { track, spool: helper_spool };
                    join_set.spawn(
                        fetch_one_slice(
                            ctx.peer_manager.clone(),
                            ctx.api.clone(),
                            token.clone(),
                            candidates,
                            request,
                            next_pos,
                        )
                        .in_current_span(),
                    );
                }
            }
            Ok(Err(())) => {
                if token.is_cancelled() {
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    return Err(());
                }

                if let Some(next_pos) = pos_iter.next() {
                    let helper_spool = group.spool_at(next_pos);
                    let prev_id = peers.previous.get(&helper_spool).copied();
                    let curr_id = peers.current.get(&helper_spool).copied();
                    let candidates = [
                        prev_id,
                        curr_id.filter(|id| prev_id.map_or(true, |p| p != *id)),
                    ];
                    let request = GetSliceReq { track, spool: helper_spool };
                    join_set.spawn(
                        fetch_one_slice(
                            ctx.peer_manager.clone(),
                            ctx.api.clone(),
                            token.clone(),
                            candidates,
                            request,
                            next_pos,
                        )
                        .in_current_span(),
                    );
                }
            }
            Err(_join_error) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(());
            }
        }
    }

    if slices.len() < k {
        return Err(());
    }

    Ok(slices)
}

/// Decode k peer slices back to the original blob, re-encode, extract our slice.
fn reconstruct(
    slicer: &mut Slicer<ClayCoder>,
    lost: SliceIndex,
    peer_slices: &[(SliceIndex, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    let Some((_, sample)) = peer_slices.first() else {
        return Err("no peer slices provided".into());
    };

    let metadata = SliceMetadata::from_slice(sample)
        .map_err(|error| format!("parse peer metadata failed: {error}"))?;

    slicer.set_chunk_index(metadata.chunk_index);

    let refs: Vec<(usize, &[u8])> = peer_slices
        .iter()
        .map(|(slice_index, data)| (**slice_index, data.as_slice()))
        .collect();

    let decoded = slicer
        .decode(&refs)
        .map_err(|error| format!("decode failed: {error}"))?;

    let reencoded = slicer
        .encode(&decoded)
        .map_err(|error| format!("re-encode failed: {error}"))?;

    reencoded
        .get(*lost)
        .cloned()
        .ok_or_else(|| format!("lost slice index {} out of bounds", *lost))
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SLICE_TREE_HEIGHT;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_protocol::api::ops::{GetSliceRes, PeerReq, PeerRes};
    use tape_store::ops::ObjectInfoOps;
    use tape_store::types::ObjectInfo;

    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    async fn test_context() -> TestContext {
        test_context_with_api(MemoryApi::noop()).await
    }

    async fn test_context_with_api(api: MemoryApi) -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    fn clay_blob(size: u64, slices: &[Vec<u8>]) -> BlobEncoding {
        let metadata = SliceMetadata::from_slice(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let leaves = core::array::from_fn(|index| hash_leaf(&slices[index]));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        BlobEncoding {
            size: StorageUnits::from_bytes(size),
            commitment,
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(stripe_size),
            stripe_count: StripeCount(size.div_ceil(stripe_size)),
            leaves,
        }
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let blob = clay_blob(size, slices);
        CompressedTrack {
            tape: Address::from([0; 32]),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(size),
            group: GroupIndex::containing(SPOOL),
            value_hash: blob.get_hash(),
        }
    }

    fn certified(track: Address) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch: EpochNumber(1),
            certified_epoch: Some(EpochNumber(2)),
            slot: SlotNumber(10),
        }
    }

    fn recover_state(epoch: EpochNumber) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Recover, epoch);
        for (slice, helper) in state.prev_helpers.iter_mut().enumerate() {
            *helper = Some(addr(200 + slice as u8));
        }
        state
    }

    #[tokio::test]
    async fn empty_queue() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
    }

    #[tokio::test]
    async fn skip_present() {
        let ctx = test_context().await;
        let a = addr(1);

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn full_recovery() {
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let group = GroupIndex::containing(SPOOL);
        let lost_pos = group.position_of(SPOOL).unwrap() as usize;
        let expected = slices[lost_pos].clone();

        let slices_for_api = slices.clone();
        let track = addr(1);
        let track_blob = clay_blob(1024, &slices);
        let track_info = clay_track(1024, &slices);
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                let pos = group.position_of(r.spool).unwrap() as usize;
                PeerRes::GetSlice(Ok(GetSliceRes {
                    data: slices_for_api[pos].clone(),
                }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(track_blob.clone())).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }

    #[tokio::test]
    async fn insufficient_peers() {
        let ctx = test_context().await; // noop api returns errors
        let a = addr(1);
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let slices = slicer.encode(&vec![0x11; 1024]).unwrap();
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn skips_uncertified() {
        let ctx = test_context().await;
        let a = addr(2);
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let slices = slicer.encode(&vec![0x33; 1024]).unwrap();
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();
        ctx.store
            .put_object_info(
                a,
                ObjectInfo::Valid {
                    track_address: a,
                    registered_epoch: EpochNumber(2),
                    certified_epoch: None,
                    slot: SlotNumber(10),
                },
            )
            .unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    /// Per-helper fallback can combine previous helpers from local spool state
    /// with current helpers from protocol state and recover successfully.
    #[tokio::test]
    async fn split_peers() {
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let group = GroupIndex::containing(SPOOL);
        let lost_pos = group.position_of(SPOOL).unwrap() as usize;
        let expected = slices[lost_pos].clone();

        let slices_for_api = slices.clone();
        let track = addr(1);
        let track_blob = clay_blob(1024, &slices);
        let track_info = clay_track(1024, &slices);

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                let pos = group.position_of(r.spool).unwrap() as usize;
                PeerRes::GetSlice(Ok(GetSliceRes {
                    data: slices_for_api[pos].clone(),
                }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        // Previous: only positions 0..3 → 4 helpers (< k=7)
        let mut state = SpoolState::new(SpoolStatus::Recover, EpochNumber(3));
        for pos in 0..4 {
            state.prev_helpers[pos] = Some(addr(200 + pos as u8));
        }

        ctx.store.set_spool_state(SPOOL, state).unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
    }

    #[test]
    fn reconstruct_roundtrip() {
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            2_000,
            true,
            profile,
        );
        slicer.set_chunk_index(ChunkNumber(42));

        let payload: Vec<u8> = (0..50_000).map(|i| (i % 251) as u8).collect();
        let slices = slicer.encode(&payload).unwrap();

        let lost = SliceIndex::new(0);
        let k = slicer.k();
        let peer_slices: Vec<(SliceIndex, Vec<u8>)> = slices
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != *lost)
            .take(k)
            .map(|(i, data)| (SliceIndex::new(i), data.clone()))
            .collect();

        let mut recovery_slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            2_000,
            true,
            profile,
        );
        let recovered = reconstruct(&mut recovery_slicer, lost, &peer_slices).unwrap();
        assert_eq!(recovered, slices[*lost]);
    }

    /// Only k helper positions return valid data; the rest return errors.
    /// Recovery should succeed without waiting for all positions.
    #[tokio::test]
    async fn early_stop() {
        let profile = EncodingProfile::clay_default();
        let k = profile.k() as usize;
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let group = GroupIndex::containing(SPOOL);
        let lost_pos = group.position_of(SPOOL).unwrap() as usize;
        let expected = slices[lost_pos].clone();

        // Compute which helper positions will succeed (first k, excluding ours).
        let mut good_spools = std::collections::HashSet::new();
        let mut count = 0;
        for pos in 0..GROUP_SIZE {
            let helper_spool = group.spool_at(pos);
            if helper_spool == SPOOL {
                continue;
            }
            if count < k {
                good_spools.insert(helper_spool);
                count += 1;
            }
        }

        let slices_for_api = slices.clone();
        let track = addr(1);
        let track_blob = clay_blob(1024, &slices);
        let track_info = clay_track(1024, &slices);
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                if good_spools.contains(&r.spool) {
                    let pos = group.position_of(r.spool).unwrap() as usize;
                    PeerRes::GetSlice(Ok(GetSliceRes {
                        data: slices_for_api[pos].clone(),
                    }))
                } else {
                    PeerRes::GetSlice(Err(tape_protocol::api::ApiError::Other(
                        "simulated failure".into(),
                    )))
                }
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }
}
