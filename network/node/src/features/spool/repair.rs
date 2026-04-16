use std::collections::HashMap;
use std::sync::Arc;

use peer_manager::PeerManager;
use rpc::Rpc;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::system::SpoolState;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::types::NodeId;
use tape_core::types::{StorageUnits, StripeCount};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::ops::RepairReq;
use tape_protocol::api::types::StripeSubChunkRequest;
use tape_retry::RetryConfig;
use tape_slicer::{ClayCoder, RepairPlan, SliceIndex, SliceMetadata, Slicer};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::policy::{track_requirement, TrackRequirement};
use crate::features::spool::types::RepairResult;

const REPAIR_FETCH_CONCURRENCY: usize = 4;

// Purpose: Bandwidth-optimal Clay repair for missing slices.
//          Drains the pending_repairs queue populated by Scan.
//          Tracks that cannot be Clay-repaired are escalated to the
//          pending_recoveries queue for the Recover task.
//
// "Escalate" means: remove from pending_repairs, add to pending_recoveries.
// Both queues are presence-based, so adds are idempotent.
//
// Algorithm:
// 1. Load spool state. Derive group and our slice index within it.
//    Build two peer maps from spool_state.prev_helpers (previous)
//    and protocol.group_peers(group) (current).
//    Exclude our own spool from both maps.
//
// 2. Batch loop over store.iter_pending_repairs(spool, batch_size):
//
//    For each track_address:
//      a. Check cancellation.
//      b. Skip if slice already present (has_slice). Remove from pending_repairs.
//      c. Load track_info. If missing, remove from pending_repairs, continue.
//      d. Validate encoding is Clay and stripe params are non-zero.
//         If not → escalate, continue.
//
//      e. Build repair plan:
//         - ClayCoder::from_params(track_info.profile().clay_params())
//         - Slicer::with_profile(coder, stripe_size, rotated=true, profile)
//         - slicer.repair_plan_from_params(lost, &available, original_size, stripe_size)
//         - If plan fails → escalate, continue.
//
//      f. Invert plan into per-helper RepairReq:
//         Group plan.stripes by helper slice → HashMap<SliceIndex, RepairReq>
//         Each RepairReq has the helper's spool and the StripeSubChunkRequests.
//
//      g. Fetch sub-chunks (per-track: per-helper fallback across both peer maps):
//         - For each helper in the plan, try the previous peer map first,
//           fall back to the current peer map. Keep the first success per helper.
//           Accumulate across both sources.
//         - If any required helper is unavailable in both maps → escalate, continue.
//
//      h. Reconstruct:
//         - SliceMetadata::with_profile(original_size, stripe_size, profile)
//         - slicer.repair(&plan, &helper_data, &metadata_bytes)
//         - If decode fails → escalate, continue.
//
//      i. Validate against track_info.verify_slice(our_position, &data).
//         If invalid → escalate, continue.
//
//      j. Persist: store.put_slice(spool, track_address, data).
//         Remove from pending_repairs.
//
// 3. Return Done { unrepairable }, count of tracks escalated.
//
// NOTE:
//
// The spool relationships are:
//   - SpoolGroup::of(spool) → group is derived from spool
//   - group.slice_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the SpoolGroup and the SliceIndex within it. So passing
//   spool, group, AND lost is redundant, any one of these plus spool is computable from the other.
//   The helpers should just take spool and derive what they need.

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> RepairResult {

    let spool_state = match ctx.store.get_spool_state(spool) {
        Ok(Some(state)) => state,
        _ => return RepairResult::Done { unrepairable: 0 },
    };

    let peers = group_peers(ctx.as_ref(), &spool_state, spool);

    let mut unrepairable = 0usize;

    loop {
        if token.is_cancelled() {
            break;
        }

        let pending = match ctx
            .store
            .iter_pending_repairs(spool, config.repair_batch.max(1))
        {
            Ok(pending) => pending,
            Err(error) => {
                warn!(spool, %error, "iter_pending_repairs failed");
                break;
            }
        };

        if pending.is_empty() {
            break;
        }

        for track in pending {
            if token.is_cancelled() {
                break;
            }

            let has_slice = match ctx.store.has_slice(spool, track) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool, track = %track, %error, "has_slice failed");
                    continue;
                }
            };

            // If slice already exists, just remove from pending_repairs and skip.
            if has_slice {
                let _ = ctx.store.remove_pending_repair(spool, track);
                info!(spool, track = %track, "slice already present, skipping");
                continue;
            }

            // Load track_info. If missing, remove from pending_repairs and skip.
            let track_info = match ctx.store.get_track(track) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    warn!(spool, track = %track, "track_info missing, removing");
                    continue;
                }
                Err(error) => {
                    warn!(spool, track = %track, %error, "get_track failed");
                    continue;
                }
            };

            if !track_info.is_blob() {
                warn!(spool, track = %track, "non-blob track in repair queue");
                continue;
            }

            let track_data = match ctx.store.get_track_data(track) {
                Ok(Some(TrackData::Blob(info))) => info,
                Ok(Some(TrackData::Raw(_))) => {
                    warn!(spool, track = %track, "blob track has raw track_data, keeping queued");
                    continue;
                }
                Ok(None) => {
                    warn!(spool, track = %track, "track_data missing, keeping queued");
                    continue;
                }
                Err(error) => {
                    warn!(spool, track = %track, %error, "get_track_data failed");
                    continue;
                }
            };

            // Only repair certified tracks.
            match track_requirement(ctx.store.as_ref(), track) {
                Ok(TrackRequirement::Required) => {}
                Ok(TrackRequirement::NotRequired) => {
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    continue;
                }
                Ok(TrackRequirement::Inconsistent) | Err(_) => {
                    warn!(spool, track = %track, "repair: skipping, state inconsistent or unreadable");
                    continue;
                }
            }

            match repair_track(ctx.as_ref(), config, spool, &peers, track, &track_data, token).await {
                Ok(data) => {
                    let repaired_len = data.len() as u64;
                    if let Err(error) = ctx.store.put_slice(spool, track, data) {
                        warn!(spool, track = %track, %error, "put_slice failed");
                        continue;
                    }
                    ctx.metrics.add_repair_persisted(repaired_len);
                    let _ = ctx.store.remove_pending_repair(spool, track);
                }
                Err(()) => {
                    info!(spool, track = %track, "repair failed, escalating to recovery");
                    match ctx.store.add_pending_recovery(spool, track) {
                        Ok(()) => {
                            let _ = ctx.store.remove_pending_repair(spool, track);
                        }
                        Err(error) => {
                            warn!(spool, track = %track, %error, "add_pending_recovery failed, keeping in repair");
                        }
                    }
                    ctx.metrics.inc_repair_escalations();
                    unrepairable += 1;
                }
            }
        }
    }

    RepairResult::Done { unrepairable }
}

/// Two peer maps: previous epoch helpers and current committee assignments.
pub struct GroupPeers {
    pub previous: HashMap<SpoolIndex, NodeId>,
    pub current: HashMap<SpoolIndex, NodeId>,
}

/// Build peer maps for a spool's group, excluding our own spool.
///
/// Node2 does not yet have its own peer-refresh/malicious-peer policy, so the
/// current committee map is taken directly from protocol state instead of
/// prefiltering via PeerManager health cooldowns.
pub fn group_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool_state: &SpoolState,
    spool: SpoolIndex,
) -> GroupPeers {
    let group = SpoolGroup::of(spool);
    let previous = spool_state
        .prev_helpers
        .iter()
        .enumerate()
        .filter_map(|(slice, node_id)| {
            let helper_spool = group.spool_at(slice);
            if helper_spool == spool {
                None
            } else {
                node_id.map(|node_id| (helper_spool, node_id))
            }
        })
        .collect();

    let protocol = ctx.state();
    let current = protocol
        .group_peers(group)
        .into_iter()
        .filter(|(helper_spool, _)| *helper_spool != spool)
        .collect();

    GroupPeers { previous, current }
}

/// Attempt Clay repair for a single track.
/// Returns Ok(repaired_data) or Err(()) to signal escalation.
async fn repair_track<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    _config: &RecoveryConfig,
    spool: SpoolIndex,
    peers: &GroupPeers,
    track: Address,
    track_data: &BlobInfo,
    token: &CancellationToken,
) -> Result<Vec<u8>, ()> {

    let profile = track_data.profile;
    if !profile.is_clay()
        || track_data.stripe_size == StorageUnits::zero()
        || track_data.stripe_count == StripeCount::zero()
    {
        return Err(());
    }

    let group = SpoolGroup::of(spool);
    let position = group.slice_of(spool).ok_or(())?;
    let lost = SliceIndex::new(position as usize);

    // Merge previous and current helpers, excluding duplicates and our own slice. 
    let mut available: Vec<SliceIndex> = peers
        .previous
        .keys()
        .chain(peers.current.keys())
        .filter_map(|helper_spool| {
            group
                .slice_of(*helper_spool)
                .map(|helper_slice| SliceIndex::new(helper_slice as usize))
        })
        .collect();

    available.sort_unstable();
    available.dedup();

    if available.is_empty() {
        return Err(());
    }

    let slicer = Slicer::with_profile(
        ClayCoder::from_params(profile.clay_params()),
        track_data.stripe_size.as_usize(),
        true,
        profile,
    );

    let plan = slicer
        .repair_plan_from_params(
            lost,
            &available,
            track_data.size.0 as usize,
            track_data.stripe_size.as_usize(),
        )
        .map_err(|_| ())?;

    let helper_data = fetch_helpers(
        ctx, spool, &plan, peers, track, token,
    ).await?;

    let metadata = SliceMetadata::with_profile(
        track_data.size.0 as usize,
        track_data.stripe_size.as_usize(),
        profile,
    )
    .to_bytes();

    let repaired = slicer.repair(&plan, &helper_data, &metadata).map_err(|_| ())?;
    if !track_data.verify_slice(position, &repaired) {
        return Err(());
    }

    Ok(repaired)
}

/// Fetch sub-chunk data from one helper using per-helper fallback.
///
/// Tries the previous peer first, then the current.
/// Returns the fetched data on first success, or the slice index on failure.
async fn fetch_one_helper<Cluster: Api + 'static>(
    peer_manager: Arc<PeerManager>,
    api: Arc<Cluster>,
    token: CancellationToken,
    candidates: [Option<NodeId>; 2],
    req: RepairReq,
    slice_idx: SliceIndex,
) -> Result<(SliceIndex, Vec<u8>), SliceIndex> {
    for node_id in candidates.into_iter().flatten() {
        if let Ok(res) = call_peer(
            &peer_manager,
            RetryConfig::three(),
            node_id,
            Some(&token),
            || api.repair(node_id, &req),
        ).await {
            return Ok((slice_idx, res.data));
        }
    }
    Err(slice_idx)
}

/// Fetch sub-chunk data from all helpers in the plan using bounded concurrency.
///
/// For each helper, tries the previous peer map first, then the current.
/// Runs up to REPAIR_FETCH_CONCURRENCY helper fetches in parallel.
/// Returns Err if any required helper is unavailable in both maps.
async fn fetch_helpers<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    plan: &RepairPlan,
    peers: &GroupPeers,
    track: Address,
    token: &CancellationToken,
) -> Result<HashMap<SliceIndex, Vec<u8>>, ()> {
    let reqs = per_helper_reqs(plan, spool, track);
    let mut work: Vec<(SliceIndex, RepairReq)> = reqs.into_iter().collect();
    work.sort_by_key(|(idx, _)| *idx);
    let mut work_iter = work.into_iter();

    let mut helper_data = HashMap::new();
    let mut join_set: JoinSet<Result<(SliceIndex, Vec<u8>), SliceIndex>> = JoinSet::new();

    // Seed initial batch.
    for _ in 0..REPAIR_FETCH_CONCURRENCY {
        if token.is_cancelled() {
            return Err(());
        }
        let Some((slice_idx, req)) = work_iter.next() else { break };
        let prev_id = peers.previous.get(&req.helper_spool).copied();
        let curr_id = peers.current.get(&req.helper_spool).copied();
        let candidates = [
            prev_id,
            curr_id.filter(|id| prev_id.map_or(true, |p| p != *id)),
        ];
        join_set.spawn(fetch_one_helper(
            ctx.peer_manager.clone(),
            ctx.api.clone(),
            token.clone(),
            candidates,
            req,
            slice_idx,
        ));
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok((slice_idx, data))) => {
                ctx.metrics.add_repair_fetched(data.len() as u64);
                helper_data.insert(slice_idx, data);

                if token.is_cancelled() {
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    return Err(());
                }

                if let Some((next_idx, next_req)) = work_iter.next() {
                    let prev_id = peers.previous.get(&next_req.helper_spool).copied();
                    let curr_id = peers.current.get(&next_req.helper_spool).copied();
                    let candidates = [
                        prev_id,
                        curr_id.filter(|id| prev_id.map_or(true, |p| p != *id)),
                    ];
                    join_set.spawn(fetch_one_helper(
                        ctx.peer_manager.clone(),
                        ctx.api.clone(),
                        token.clone(),
                        candidates,
                        next_req,
                        next_idx,
                    ));
                }
            }
            Ok(Err(_failed_idx)) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(());
            }
            Err(_join_error) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(());
            }
        }
    }

    Ok(helper_data)
}

/// Invert a RepairPlan into per-helper RepairReqs.
fn per_helper_reqs(
    plan: &RepairPlan,
    spool: SpoolIndex,
    track: Address,
) -> HashMap<SliceIndex, RepairReq> {
    let group = SpoolGroup::of(spool);
    let mut reqs: HashMap<SliceIndex, RepairReq> = HashMap::new();

    for stripe_repair in &plan.stripes {
        for helper in &stripe_repair.helpers {
            let entry = reqs.entry(helper.slice).or_insert_with(|| RepairReq {
                track: track.into(),
                helper_spool: group.spool_at(*helper.slice),
                stripes: vec![],
            });
            entry.stripes.push(StripeSubChunkRequest {
                stripe: stripe_repair.stripe,
                sub_chunks: helper.sub_chunks.clone(),
            });
        }
    }

    reqs
}


/// Extract sub-chunk data from a local slice to serve a repair request.
/// Called by the HTTP handler when a peer asks for repair data.
pub fn extract_repair_data(
    track_info: &BlobInfo,
    stripes: &[StripeSubChunkRequest],
    slice_data: &[u8],
) -> Result<Vec<u8>, String> {
    let profile = track_info.profile;
    if !profile.is_clay() {
        return Err("repair only supported for clay tracks".into());
    }

    let coder = ClayCoder::from_params(profile.clay_params());
    let metadata = SliceMetadata::from_slice(slice_data)
        .map_err(|error| format!("parse slice metadata failed: {error}"))?;

    let num_stripes = if metadata.blob_len() == 0 {
        1
    } else {
        metadata.blob_len().div_ceil(metadata.stripe_size())
    };

    let total_data_len = slice_data
        .len()
        .checked_sub(SliceMetadata::SIZE)
        .ok_or_else(|| "slice too short for metadata".to_string())?;

    if total_data_len == 0 || total_data_len % num_stripes != 0 {
        return Err("slice layout is inconsistent".into());
    }

    let chunk_size = total_data_len / num_stripes;

    let alpha = coder.alpha();
    if chunk_size % alpha != 0 {
        return Err("chunk size is not divisible by alpha".into());
    }

    let sub_chunk_size = chunk_size / alpha;

    let mut out = Vec::new();
    for stripe_req in stripes {
        let chunk_start = stripe_req.stripe as usize * chunk_size;
        let chunk_end = chunk_start + chunk_size;
        let chunk = slice_data
            .get(chunk_start..chunk_end)
            .ok_or_else(|| "slice too short for requested stripe".to_string())?;

        for &sc_idx in &stripe_req.sub_chunks {
            let start = sc_idx as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            let sc = chunk
                .get(start..end)
                .ok_or_else(|| "sub-chunk out of bounds".to_string())?;
            out.extend_from_slice(sc);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::COMMITMENT_TREE_HEIGHT;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_protocol::api::ops::{PeerReq, PeerRes, RepairRes};
    use tape_slicer::{ClayCoder, ErasureCoder, Slicer};
    use tape_core::track::blob::BlobInfo;
    use tape_store::ops::ObjectInfoOps;
    use tape_store::types::ObjectInfo;
    use tape_core::system::SpoolStatus;

    use crate::context::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let blob = clay_blob(size, slices);
        CompressedTrack {
            tape: Address::from([0; 32]),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(size),
            spool_group: SpoolGroup::of(SPOOL),
            value_hash: blob.get_hash(),
        }
    }

    fn clay_blob(size: u64, slices: &[Vec<u8>]) -> BlobInfo {
        let metadata = SliceMetadata::from_slice(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let leaves = core::array::from_fn(|index| hash_leaf(&slices[index]));
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);
        BlobInfo {
            size: StorageUnits::from_bytes(size),
            commitment,
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(stripe_size),
            stripe_count: StripeCount(size.div_ceil(stripe_size)),
            leaves,
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

    fn repair_state(epoch: EpochNumber) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Repair, epoch);
        for (slice, helper) in state.prev_helpers.iter_mut().enumerate() {
            *helper = Some(NodeId(100 + slice as u64));
        }
        state
    }

    #[tokio::test]
    async fn empty_queue() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
    }

    #[tokio::test]
    async fn skip_present() {
        let ctx = test_context();
        let a = addr(1);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_slice(SPOOL, a, vec![0xAB; 64]).unwrap();
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn clay_repair() {
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let track = addr(9);
        let group = SpoolGroup::of(SPOOL);
        let lost_pos = group.slice_of(SPOOL).unwrap() as usize;
        let expected = slices[lost_pos].clone();
        let track_info = clay_track(1024, &slices);
        let track_blob = clay_blob(1024, &slices);
        let track_blob_for_api = track_blob.clone();
        let slices_for_api = slices.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                let helper_slice = &slices_for_api[group.slice_of(req.helper_spool).unwrap() as usize];

                let data = extract_repair_data(
                    &track_blob_for_api,
                    &req.stripes,
                    helper_slice,
                ).unwrap();

                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        }));

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, TrackData::Blob(track_blob.clone())).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    #[tokio::test]
    async fn escalates_failure() {
        let ctx = test_context(); // noop api, no peers
        let a = addr(1);
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let slices = slicer.encode(&vec![0x24; 1024]).unwrap();
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_track_data(a, TrackData::Blob(track_blob)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn missing_track() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.add_pending_repair(SPOOL, addr(1)).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
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
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_track_data(a, TrackData::Blob(track_blob)).unwrap();
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
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    /// The repair plan requires d=16 helpers. Previous map covers positions 0..9 (excluding 5),
    /// current map covers positions 10..19. Neither alone satisfies all plan helpers.
    /// Per-helper fallback finds each helper in whichever map has it.
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
        let track = addr(9);
        let group = SpoolGroup::of(SPOOL);
        let lost_pos = group.slice_of(SPOOL).unwrap() as usize;
        let expected = slices[lost_pos].clone();
        let track_info = clay_track(1024, &slices);
        let track_blob = clay_blob(1024, &slices);
        let track_blob_for_api = track_blob.clone();
        let slices_for_api = slices.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                let helper_slice = &slices_for_api[group.slice_of(req.helper_spool).unwrap() as usize];
                let data = extract_repair_data(
                    &track_blob_for_api,
                    &req.stripes,
                    helper_slice,
                ).unwrap();
                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        }));

        // Previous: positions 0..9 (excluding 5) → 9 helpers
        let mut state = SpoolState::new(SpoolStatus::Repair, EpochNumber(3));
        for pos in 0..10 {
            if pos != 5 {
                state.prev_helpers[pos] = Some(NodeId(100 + pos as u64));
            }
        }

        // Current: positions 10..19 → 10 helpers
        let mut protocol = ProtocolState::default();
        for i in 0..10u64 {
            protocol
                .committee
                .push(CommitteeMember::new(NodeId(300 + i), Coin::<TAPE>::new(1000)));
        }
        let mut mapping = [255u8; SPOOL_COUNT];
        for i in 0..10 {
            mapping[group.spool_at(10 + i) as usize] = i as u8;
        }
        protocol.spools = SpoolAssignment::new(mapping);
        ctx.set_state(protocol).unwrap();

        ctx.store.set_spool_state(SPOOL, state).unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, TrackData::Blob(track_blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }
}
