use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::Api;
use tape_slicer::{ClayCoder, ErasureCoder, SliceIndex, Slicer};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::features::spool::types::RecoverResult;

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
//      d. Fetch k full slices (per-track: try prev_helpers first, then current):
//         - Build ordered list of peer spools (excluding ours).
//         - Try fetching all from previous peer map via call_peer + api.get_slice.
//         - If we got >= k valid slices, proceed. Otherwise discard all and
//           retry the entire set from the current peer map.
//         - If still < k → track stays pending, continue.
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

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    cancel: &CancellationToken,
) -> RecoverResult {
    todo!()
}

/// Fetch k full slices from a peer map for a given track.
/// Returns collected (slice_index, data) pairs, or Err if < k available.
async fn fetch_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    k: usize,
    peer_map: &HashMap<SpoolIndex, NodeId>,
    track_addr: Pubkey,
) -> Result<Vec<(SliceIndex, Vec<u8>)>, ()> {
    todo!()
}

/// Decode k peer slices back to the original blob, re-encode, extract our slice.
fn reconstruct(
    slicer: &mut Slicer<ClayCoder>,
    lost: SliceIndex,
    peer_slices: &[(SliceIndex, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::EpochNumber;
    use tape_protocol::api::ops::{GetSliceRes, PeerReq, PeerRes};
    use tape_store::types::{SpoolState, SpoolStatus};

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

    fn recover_state(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Recover, epoch)
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
        ctx.store
            .put_track(addr(1), clay_track(1024, &slices))
            .unwrap();
        ctx.store.add_pending_recovery(SPOOL, addr(1)).unwrap();

        // todo: set up spool_state.prev_helpers or current peers so
        // fetch_slices can find them. Then:
        // let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        // assert_eq!(result, RecoverResult::Done { remaining: 0 });
        // assert_eq!(ctx.store.get_slice(SPOOL, addr(1)).unwrap().unwrap(), expected);
        // assert!(!ctx.store.has_pending_recovery(SPOOL, addr(1)).unwrap());
        todo!()
    }

    #[tokio::test]
    async fn insufficient_peers() {
        let ctx = test_context(); // noop api returns errors
        let a = addr(1);

        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();
        ctx.store
            .put_track(a, clay_track(1024, &vec![vec![]; 20]))
            .unwrap();
        ctx.store.add_pending_recovery(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &SpoolManagerConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { remaining: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
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
}
