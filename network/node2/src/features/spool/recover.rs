use std::sync::Arc;

use peer_manager::PeerManager;
use rpc::Rpc;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_protocol::api::ops::GetSliceReq;
use tape_retry::RetryConfig;
use tape_slicer::{ClayCoder, ErasureCoder, SliceIndex, SliceMetadata, Slicer};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::Pubkey;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::SpoolManagerConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::policy::{track_requirement, TrackRequirement};
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
//   - SpoolGroup::of(spool) → group is derived from spool
//   - group.slice_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the SpoolGroup and the SliceIndex within it. So passing
//   spool, group, AND lost is redundant — any one of these plus spool is computable from the other.
//   The helpers should just take spool and derive what they need.

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> RecoverResult {

    let Some(spool_state) = ctx.store.get_spool_state(spool).ok().flatten() else {
        return RecoverResult::Done { remaining: 0 };
    };

    let peers = group_peers(ctx.as_ref(), &spool_state, spool);
    let group = SpoolGroup::of(spool);
    let position = group.slice_of(spool).unwrap_or_default();
    let batch_size = config.recover_batch_size.max(1);

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
                warn!(spool, %error, "iter_pending_recoveries failed");
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
                    warn!(spool, track = %track_addr, %error, "has_slice failed");
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
                    warn!(spool, track = %track_addr, %error, "get_track failed");
                    continue;
                }
            };

            match track_requirement(ctx.store.as_ref(), track_addr) {
                Ok(TrackRequirement::Required) => {}
                Ok(TrackRequirement::NotRequired) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    made_progress = true;
                    continue;
                }
                Ok(TrackRequirement::Inconsistent) | Err(_) => {
                    warn!(spool, track = %track_addr, "recover: skipping, state inconsistent or unreadable");
                    continue;
                }
            }

            let profile = track_info.profile();
            if !profile.is_clay() || track_info.stripe_size == 0 {
                continue;
            }

            let mut slicer = Slicer::with_profile(
                ClayCoder::from_params(profile.clay_params()),
                track_info.stripe_size as usize,
                true,
                profile,
            );
            let k = slicer.k();

            let peer_slices = match fetch_slices(
                ctx.as_ref(), config, spool, k, &peers, track_addr, token
            ).await
            {
                Ok(peer_slices) => peer_slices,
                Err(()) => continue,
            };

            let recovered =
                match reconstruct(&mut slicer, SliceIndex::new(position), &peer_slices) {
                    Ok(recovered) => recovered,
                    Err(error) => {
                        debug!(spool, track = %track_addr, %error, "reconstruct failed");
                        continue;
                    }
                };

            if !track_info.verify_slice(position, &recovered) {
                continue;
            }

            let recovered_len = recovered.len() as u64;
            if let Err(error) = ctx.store.put_slice(spool, track_addr, recovered) {
                warn!(spool, track = %track_addr, %error, "put_slice failed");
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
    retry: RetryConfig,
    token: CancellationToken,
    candidates: [Option<NodeId>; 2],
    request: GetSliceReq,
    helper_slice: usize,
) -> Result<(SliceIndex, Vec<u8>), ()> {
    for node_id in candidates.into_iter().flatten() {
        if let Ok(res) = call_peer(
            &peer_manager,
            retry.clone(),
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
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    k: usize,
    peers: &GroupPeers,
    track_addr: Pubkey,
    token: &CancellationToken,
) -> Result<Vec<(SliceIndex, Vec<u8>)>, ()> {

    let group = SpoolGroup::of(spool);
    let track: tape_crypto::Pubkey = track_addr.into();
    let mut slices = Vec::with_capacity(k);

    let positions: Vec<usize> = (0..SPOOL_GROUP_SIZE)
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
        join_set.spawn(fetch_one_slice(
            ctx.peer_manager.clone(),
            ctx.api.clone(),
            config.peer_retry.clone(),
            token.clone(),
            candidates,
            request,
            helper_slice,
        ));
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
                    join_set.spawn(fetch_one_slice(
                        ctx.peer_manager.clone(),
                        ctx.api.clone(),
                        config.peer_retry.clone(),
                        token.clone(),
                        candidates,
                        request,
                        next_pos,
                    ));
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
                    join_set.spawn(fetch_one_slice(
                        ctx.peer_manager.clone(),
                        ctx.api.clone(),
                        config.peer_retry.clone(),
                        token.clone(),
                        candidates,
                        request,
                        next_pos,
                    ));
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
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{EpochNumber, NodeId, SlotNumber};
    use tape_protocol::api::ops::{GetSliceRes, PeerReq, PeerRes};
    use tape_store::ops::ObjectInfoOps;
    use tape_store::types::{ObjectInfo, SpoolState, SpoolStatus, TrackInfo};

    use crate::context::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;

    fn addr(n: u8) -> Pubkey {
        Pubkey([n; 32])
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> TrackInfo {
        let profile = EncodingProfile::clay_default();
        let metadata = SliceMetadata::from_slice(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let commitment = slices
            .iter()
            .map(|s| tape_crypto::merkle::hash_leaf(s))
            .collect();
        TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group: SpoolGroup::of(SPOOL),
            original_size: size,
            stripe_size,
            stripe_count: size.div_ceil(stripe_size),
            encoding_type: profile.encoding as u64,
            encoding_params: profile.params,
            commitment,
        }
    }

    fn certified(track: Pubkey) -> ObjectInfo {
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
            *helper = Some(NodeId(200 + slice as u64));
        }
        state
    }

    #[tokio::test]
    async fn empty_queue() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();

        let result = run(ctx, &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
    }

    #[tokio::test]
    async fn skip_present() {
        let ctx = test_context();
        let a = addr(1);

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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
        let group = SpoolGroup::of(SPOOL);
        let lost_pos = group.slice_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();

        let slices_for_api = slices.clone();
        let track = addr(1);
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                let pos = group.slice_of(r.spool).unwrap();
                PeerRes::GetSlice(Ok(GetSliceRes {
                    data: slices_for_api[pos].clone(),
                }))
            }
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }

    #[tokio::test]
    async fn insufficient_peers() {
        let ctx = test_context(); // noop api returns errors
        let a = addr(1);
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let slices = slicer.encode(&vec![0x11; 1024]).unwrap();

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn skips_uncertified() {
        let ctx = test_context();
        let a = addr(2);
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let slices = slicer.encode(&vec![0x33; 1024]).unwrap();

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
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

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    /// Neither previous nor current alone has k=7 helpers, but combined they do.
    /// Previous has positions 0..3 (4 helpers), current has positions 15..19 (5 helpers).
    /// Per-helper fallback collects from both sources and recovers successfully.
    #[tokio::test]
    async fn split_peers() {
        use tape_core::erasure::SPOOL_COUNT;
        use tape_core::spooler::SpoolAssignment;
        use tape_core::system::CommitteeMember;
        use tape_core::types::coin::{Coin, TAPE};
        use tape_protocol::ProtocolState;

        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let group = SpoolGroup::of(SPOOL);
        let lost_pos = group.slice_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();

        let slices_for_api = slices.clone();
        let track = addr(1);

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                let pos = group.slice_of(r.spool).unwrap();
                PeerRes::GetSlice(Ok(GetSliceRes {
                    data: slices_for_api[pos].clone(),
                }))
            }
            _ => panic!("unexpected request"),
        }));

        // Previous: only positions 0..3 → 4 helpers (< k=7)
        let mut state = SpoolState::new(SpoolStatus::Recover, EpochNumber(3));
        for pos in 0..4 {
            state.prev_helpers[pos] = Some(NodeId(200 + pos as u64));
        }

        // Current: only positions 15..19 → 5 helpers (< k=7)
        let mut protocol = ProtocolState::default();
        for i in 0..5u64 {
            protocol
                .committee
                .push(CommitteeMember::new(NodeId(300 + i), Coin::<TAPE>::new(1000)));
        }
        let mut mapping = [255u8; SPOOL_COUNT];
        for i in 0..5 {
            mapping[group.spool_at(15 + i) as usize] = i as u8;
        }
        protocol.spools = SpoolAssignment::new(mapping);
        ctx.set_state(protocol).unwrap();

        ctx.store.set_spool_state(SPOOL, state).unwrap();
        ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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
        slicer.set_chunk_index(42);

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
        let group = SpoolGroup::of(SPOOL);
        let lost_pos = group.slice_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();

        // Compute which helper positions will succeed (first k, excluding ours).
        let mut good_spools = std::collections::HashSet::new();
        let mut count = 0;
        for pos in 0..SPOOL_GROUP_SIZE {
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
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) => {
                if good_spools.contains(&r.spool) {
                    let pos = group.slice_of(r.spool).unwrap();
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
        }));

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }
}
