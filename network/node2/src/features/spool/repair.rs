use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_protocol::api::ops::RepairReq;
use tape_slicer::{RepairPlan, SliceIndex, Slicer};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey, SpoolState, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
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
    cancel: &CancellationToken,
) -> RepairResult {
    todo!()
}

/// Two peer maps: previous epoch helpers and current committee assignments.
struct GroupPeers {
    previous: HashMap<SpoolIndex, NodeId>,
    current: HashMap<SpoolIndex, NodeId>,
}

/// Build peer maps for a spool's group, excluding our own spool.
fn group_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool_state: &SpoolState,
    spool: SpoolIndex,
) -> GroupPeers {
    todo!()
}

/// Attempt Clay repair for a single track.
/// Returns Ok(repaired_data) or Err(()) to signal escalation.
async fn repair_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    peers: &GroupPeers,
    track_addr: Pubkey,
    track_info: &TrackInfo,
) -> Result<Vec<u8>, ()> {
    todo!()
}

/// Fetch sub-chunk data from all helpers in the plan using one peer map.
/// Returns the collected helper data, or Err if any helper failed.
async fn fetch_helpers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    plan: &RepairPlan,
    peer_map: &HashMap<SpoolIndex, NodeId>,
    track_addr: Pubkey,
) -> Result<HashMap<SliceIndex, Vec<u8>>, ()> {
    todo!()
}

/// Invert a RepairPlan into per-helper RepairReqs.
fn per_helper_reqs(plan: &RepairPlan, spool: SpoolIndex) -> HashMap<SliceIndex, RepairReq> {
    todo!()
}

/// Remove from pending_repairs, add to pending_recoveries.
fn escalate<Db: Store>(store: &tape_store::TapeStore<Db>, spool: SpoolIndex, track: Pubkey) {
    todo!()
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

    use crate::core::context::test_utils::{test_context, test_context_with_api};

    const SPOOL: SpoolIndex = 5;

    fn addr(n: u8) -> Pubkey {
        Pubkey([n; 32])
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> TrackInfo {
        let profile = EncodingProfile::clay_default();
        let commitment = slices
            .iter()
            .map(|s| tape_crypto::merkle::hash_leaf(s))
            .collect();
        TrackInfo {
            tape_address: Pubkey([0; 32]),
            spool_group: SpoolGroup::of(SPOOL),
            original_size: size,
            stripe_size: 512,
            stripe_count: (size + 511) / 512,
            encoding_type: profile.encoding as u64,
            encoding_params: profile.params,
            commitment,
        }
    }

    fn repair_state(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Repair, epoch)
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
        // ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        // assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn clay_repair() {
        // Encode a blob, remove one slice, mock peers returning repair data.
        // Verify the repaired slice matches the original and is persisted.
        let profile = EncodingProfile::clay_default();
        let mut slicer = Slicer::with_profile(
            ClayCoder::from_params(profile.clay_params()),
            512,
            true,
            profile,
        );
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let lost_pos = SpoolGroup::of(SPOOL).slice_of(SPOOL).unwrap();
        let _expected = slices[lost_pos].clone();
        let _track_info = clay_track(1024, &slices);

        // todo: wire up MemoryApi to return repair sub-chunks,
        // set up spool state with prev_helpers, run repair, verify slice persisted.
        todo!()
    }

    #[tokio::test]
    async fn escalates_failure() {
        // No peers available → track should be moved to pending_recoveries.
        let ctx = test_context(); // noop api, no peers
        let a = addr(1);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store
            .put_track(a, clay_track(1024, &vec![vec![]; 20]))
            .unwrap();
        // ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        // assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
        // assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn missing_track() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        // ctx.store.add_pending_repair(SPOOL, addr(1)).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        // assert!(!ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
    }
}
