use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_protocol::api::ops::RepairReq;
use tape_protocol::api::types::StripeSubChunkRequest;
use tape_slicer::{ClayCoder, RepairPlan, SliceIndex, SliceMetadata, Slicer};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey, SpoolState, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::SpoolManagerConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::types::RepairResult;

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
//    and peer_manager.healthy_peers_for_group (current).
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
//      g. Fetch sub-chunks (per-track: try prev_helpers first, then current):
//         - For each helper in the plan, send RepairReq via call_peer
//           using the previous peer map.
//         - If ANY helper from the previous set fails or is missing,
//           discard all previous results for this track and retry
//           the entire helper set using the current peer map.
//         - If still missing required helpers → escalate, continue.
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
// 3. Return Done { unrepairable } — count of tracks escalated.
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

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
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
            .iter_pending_repairs(spool, config.repair_batch_size.max(1))
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
            let Some(track_info) = ctx.store.get_track(track).ok().flatten() else {
                let _ = ctx.store.remove_pending_repair(spool, track);
                warn!(spool, track = %track, "track_info missing, skipping");
                continue;
            };

            match repair_track(ctx.as_ref(), config, spool, &peers, track, &track_info, token).await {
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
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    let _ = ctx.store.add_pending_recovery(spool, track);
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
    let current = ctx
        .peer_manager
        .healthy_peers_for_group(protocol.as_ref(), group)
        .into_iter()
        .filter(|(helper_spool, _)| *helper_spool != spool)
        .collect();

    GroupPeers { previous, current }
}

/// Attempt Clay repair for a single track.
/// Returns Ok(repaired_data) or Err(()) to signal escalation.
async fn repair_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    peers: &GroupPeers,
    track: Pubkey,
    track_info: &TrackInfo,
    token: &CancellationToken,
) -> Result<Vec<u8>, ()> {

    let profile = track_info.profile();
    if !profile.is_clay() || track_info.stripe_size == 0 || track_info.stripe_count == 0 {
        return Err(());
    }

    let group = SpoolGroup::of(spool);
    let position = group.slice_of(spool).ok_or(())?;
    let lost = SliceIndex::new(position);

    // Merge previous and current helpers, excluding duplicates and our own slice. 
    let mut available: Vec<SliceIndex> = peers
        .previous
        .keys()
        .chain(peers.current.keys())
        .filter_map(|helper_spool| group.slice_of(*helper_spool).map(SliceIndex::new))
        .collect();

    available.sort_unstable();
    available.dedup();

    if available.is_empty() {
        return Err(());
    }

    let slicer = Slicer::with_profile(
        ClayCoder::from_params(profile.clay_params()),
        track_info.stripe_size as usize,
        true,
        profile,
    );

    let plan = slicer
        .repair_plan_from_params(
            lost,
            &available,
            track_info.original_size as usize,
            track_info.stripe_size as usize,
        )
        .map_err(|_| ())?;

    // Try fetching helper data using the previous peer map first, then fall back to current if any
    // are missing. There is a chance that the difference between the two maps could cause the
    // repair to fail even if helpers are available.
    let helper_data = match fetch_helpers(
        ctx, config, spool, &plan, &peers.previous, track, token,
    ).await {
        Ok(helper_data) => helper_data,
        Err(()) => fetch_helpers(
            ctx, config, spool, &plan, &peers.current, track, token,
        ).await?,
    };

    let metadata = SliceMetadata::with_profile(
        track_info.original_size as usize,
        track_info.stripe_size as usize,
        profile,
    )
    .to_bytes();

    let repaired = slicer.repair(&plan, &helper_data, &metadata).map_err(|_| ())?;
    if !track_info.verify_slice(position, &repaired) {
        return Err(());
    }

    Ok(repaired)
}

/// Fetch sub-chunk data from all helpers in the plan using one peer map.
/// Returns the collected helper data, or Err if any helper failed.
async fn fetch_helpers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    plan: &RepairPlan,
    peer_map: &HashMap<SpoolIndex, NodeId>,
    track: Pubkey,
    token: &CancellationToken,
) -> Result<HashMap<SliceIndex, Vec<u8>>, ()> {
    let reqs = per_helper_reqs(plan, spool, track);
    let mut helper_data = HashMap::new();

    for (slice_idx, req) in &reqs {
        let Some(&node_id) = peer_map.get(&req.helper_spool) else {
            return Err(());
        };

        let result = call_peer(
            &ctx.peer_manager,
            config.peer_retry.clone(),
            node_id,
            Some(token),
            || ctx.api.repair(node_id, req),
        )
        .await;

        match result {
            Ok(res) => {
                ctx.metrics.add_repair_fetched(res.data.len() as u64);
                helper_data.insert(*slice_idx, res.data);
            }
            Err(_) => return Err(()),
        }
    }

    Ok(helper_data)
}

/// Invert a RepairPlan into per-helper RepairReqs.
fn per_helper_reqs(
    plan: &RepairPlan,
    spool: SpoolIndex,
    track: Pubkey,
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
    track_info: &TrackInfo,
    stripes: &[StripeSubChunkRequest],
    slice_data: &[u8],
) -> Result<Vec<u8>, String> {
    let profile = track_info.profile();
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
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::types::EpochNumber;
    use tape_protocol::api::ops::{PeerReq, PeerRes, RepairRes};
    use tape_slicer::{ClayCoder, ErasureCoder, Slicer};
    use tape_store::types::SpoolStatus;

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

        let result = run(ctx, &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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
        let lost_pos = group.slice_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();
        let track_info = clay_track(1024, &slices);
        let track_info_for_api = track_info.clone();
        let slices_for_api = slices.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                let helper_slice = &slices_for_api[group.slice_of(req.helper_spool).unwrap()];

                let data = extract_repair_data(
                    &track_info_for_api,
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
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
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

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
    }
}
