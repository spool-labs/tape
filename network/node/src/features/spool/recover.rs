use std::sync::Arc;

use tape_peer_http::{TRANSFER_IDLE, with_idle_limit};
use tape_peer_manager::PeerManager;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::spooler::VirtualGroup;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::ops::GetSliceReq;
use tape_retry::RetryConfig;
use tape_slicer::{ErasureCoder, SliceIndex, Slicer, StripedCoder};
use tape_store::ops::{
    MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps, track_has_left,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::repair::{GroupPeers, group_peers};
use crate::features::spool::types::RecoverResult;

const RECOVER_FETCH_CONCURRENCY: usize = 4;

// Purpose: Full erasure code recovery for slices that could not be Clay-repaired,
//          and for every slice on a Reed-Solomon or copies network.
//          Drains the pending_recoveries queue the Repair task fills.
//
// Algorithm:
// 1. Load spool state. Derive group and our slice index.
//    Build two peer maps (previous, current), same as repair.
//
// 2. One pass over the rows due at the applied slot read when the run starts,
//    in queue order, store.recoveries_due(spool, after, until, batch_size)
//    from the last row read. A row recovery can't act on yet is put back
//    (requeue_recovery) and waits for the cursor to move.
//
//    For each track_address:
//      a. Check cancellation.
//      b. Skip if slice already present (has_slice). Remove from the queue.
//      c. Load track_info. If missing, remove from the queue, continue.
//      d. One try (RecoveryRun::try_track):
//         - Ask positions in turn from a start the track's address picks,
//           ours left out.
//         - Each holder of a position gets one try: the previous holder,
//           then the current one when it differs. A holder whose body moves
//           no byte for TRANSFER_IDLE gives up, however long it has sent.
//         - Check each slice against its leaf. A slice that fails is dropped
//           and counted, and the position's next holder, then the next
//           position, is asked.
//         - Stop at k slices that match.
//         - Decode them, re-encode, take our slice, and check it against
//           the track's leaf.
//
//      e. Recovered: store.put_slice, remove from the queue.
//         Failed: put the row back. There is no limit on tries.
//         Cancelled: the row stays as it was, and the run ends.
//
// 3. Return Done { unrecovered }, count of failed tries.
//
// NOTE:
//
// The spool relationships are:
//   - VirtualGroup::containing(spool) → group is derived from spool
//   - group.position_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the VirtualGroup and the SliceIndex within it. So passing
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
        Ok(None) => return RecoverResult::Done { unrecovered: 0 },
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read spool state");
            return RecoverResult::Done { unrecovered: 0 };
        }
    };

    let mut slicer = match Slicer::try_from_profile(ctx.params().profile) {
        Ok(slicer) => slicer,
        Err(error) => {
            warn!(spool = %spool, %error, "recover: the network profile builds no slicer");
            return RecoverResult::Done { unrecovered: 0 };
        }
    };

    // The run takes the rows due when it starts. A row put back lands past
    // this bound, so it waits for the next run.
    let until = match ctx.store.get_sync_cursor() {
        Ok(cursor) => cursor.unwrap_or_default(),
        Err(error) => {
            warn!(spool = %spool, %error, "get_sync_cursor failed");
            return RecoverResult::Done { unrecovered: 0 };
        }
    };

    let recovery_run = RecoveryRun {
        ctx: ctx.as_ref(),
        spool,
        peers: group_peers(ctx.as_ref(), &spool_state, spool),
        token,
    };

    let batch_size = config.recover_batch.max(1);

    let mut unrecovered = 0usize;
    let mut after = None;

    'pass: loop {
        if token.is_cancelled() {
            break;
        }

        let due = match ctx.store.recoveries_due(spool, after, until, batch_size) {
            Ok(due) => due,
            Err(error) => {
                warn!(spool = %spool, %error, "recoveries_due failed");
                break;
            }
        };

        if due.is_empty() {
            break;
        }

        for row in due {
            if token.is_cancelled() {
                break 'pass;
            }

            after = Some(row);
            let track_addr = row.track;

            let has_slice = match ctx.store.has_slice(spool, track_addr) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "has_slice failed");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
            };

            if has_slice {
                let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                continue;
            }

            let track_info = match ctx.store.get_track(track_addr) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "get_track failed");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
            };

            if !track_info.is_coded() {
                warn!(spool = %spool, track = %track_addr, "non-blob track in recovery queue, putting it back");
                put_back(ctx.as_ref(), spool, track_addr);
                continue;
            }

            // A track that has left its tape is deleted by the sweep, so
            // nothing recovers it.
            match ctx.store.get_tape(track_info.tape) {
                Ok(tape) if track_has_left(tape.as_ref(), &track_info) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "get_tape failed");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
            }

            let track_data = match ctx.store.get_track_data(track_addr) {
                Ok(Some(BlobData::Coded(info))) => info,
                Ok(Some(BlobData::Inline(_))) => {
                    warn!(spool = %spool, track = %track_addr, "blob track has raw track_data, putting it back");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
                Ok(None) => {
                    warn!(spool = %spool, track = %track_addr, "track_data missing, putting it back");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track_addr, %error, "get_track_data failed");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
            };

            // Only consider certified tracks for recovery
            match ctx.store.get_object_info(track_addr) {
                Ok(Some(info)) if info.is_certified() => {}
                Ok(Some(_)) => {
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                    continue;
                }
                Ok(None) | Err(_) => {
                    warn!(spool = %spool, track = %track_addr, "recover: state inconsistent or unreadable, putting it back");
                    put_back(ctx.as_ref(), spool, track_addr);
                    continue;
                }
            }

            match recovery_run.try_track(&mut slicer, track_addr, &track_data).await {
                Outcome::Recovered(recovered) => {
                    let recovered_len = recovered.len() as u64;
                    if let Err(error) = ctx.store.put_slice(spool, track_addr, recovered) {
                        warn!(spool = %spool, track = %track_addr, %error, "put_slice failed");
                        put_back(ctx.as_ref(), spool, track_addr);
                        ctx.metrics.inc_recover_put_back();
                        continue;
                    }
                    ctx.metrics.add_recover_persisted(recovered_len);
                    ctx.metrics.inc_recover_recovered();
                    let _ = ctx.store.remove_pending_recovery(spool, track_addr);
                }
                Outcome::Failed => {
                    put_back(ctx.as_ref(), spool, track_addr);
                    ctx.metrics.inc_recover_put_back();
                    unrecovered += 1;
                }
                // An epoch turn or a shutdown isn't a try, so the row stays as it was
                Outcome::Cancelled => break 'pass,
            }
        }
    }

    RecoverResult::Done { unrecovered }
}

/// Put a row back behind the queue, where it waits for the cursor to move
fn put_back<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    track: Address,
) {
    if let Err(error) = ctx.store.requeue_recovery(spool, track) {
        warn!(spool = %spool, track = %track, %error, "requeue_recovery failed");
    }
}

/// How one recovery try ended
enum Outcome {
    /// Our slice, rebuilt and checked against the track's leaf
    Recovered(Vec<u8>),
    /// Fewer than k slices matched their leaves, the decode errored, or the check failed
    Failed,
    /// An epoch turn or a shutdown stopped the try
    Cancelled,
}

/// The slices one try fetched
struct Fetched {
    /// Slices that matched their leaves, by position
    slices: Vec<(SliceIndex, Vec<u8>)>,
    /// Slices holders sent that didn't match their leaves
    rejected: usize,
}

/// What one position's holders sent
struct Answer {
    /// A slice that matched its leaf, if a holder sent one
    slice: Option<Vec<u8>>,
    /// Slices that didn't match the leaf
    rejected: usize,
    /// Bytes the holders sent
    bytes: u64,
}

/// One recovery run over a stored spool. Its tries share the context and the
/// holders.
struct RecoveryRun<'a, Db: Store, Cluster: Api, Blockchain: Rpc> {
    ctx: &'a NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    peers: GroupPeers,
    token: &'a CancellationToken,
}

impl<Db: Store, Cluster: Api + 'static, Blockchain: Rpc> RecoveryRun<'_, Db, Cluster, Blockchain> {
    /// One try: fetch k slices that match their leaves, rebuild ours, check it
    async fn try_track(
        &self,
        slicer: &mut Slicer<StripedCoder>,
        track: Address,
        track_data: &BlobEncoding,
    ) -> Outcome {
        if track_data.stripe_size == StorageUnits::zero() {
            return Outcome::Failed;
        }
        let Some(position) = VirtualGroup::containing(self.spool).position_of(self.spool) else {
            return Outcome::Failed;
        };

        let fetched = self.fetch_slices(track, track_data, position).await;
        if self.token.is_cancelled() {
            return Outcome::Cancelled;
        }
        self.ctx.metrics.add_recover_rejected(fetched.rejected as u64);

        let k = usize::from(self.ctx.params().profile.k());
        if fetched.slices.len() < k {
            info!(
                spool = %self.spool,
                track = %track,
                matched = fetched.slices.len(),
                rejected = fetched.rejected,
                "recovery try failed, too few slices"
            );
            return Outcome::Failed;
        }
        if fetched.rejected > 0 {
            info!(spool = %self.spool, track = %track, rejected = fetched.rejected, "recovery dropped slices that didn't match their leaves");
        }

        let recovered = match reconstruct(slicer, SliceIndex::new(position), &fetched.slices) {
            Ok(recovered) => recovered,
            Err(error) => {
                debug!(spool = %self.spool, track = %track, %error, "reconstruct failed");
                return Outcome::Failed;
            }
        };

        if !track_data.verify_slice(SpoolIndex::from(position as u64), &recovered) {
            warn!(spool = %self.spool, track = %track, "recovered slice doesn't match the track's leaf");
            return Outcome::Failed;
        }

        Outcome::Recovered(recovered)
    }

    /// Fetch k slices that match their leaves, up to RECOVER_FETCH_CONCURRENCY
    /// positions at once, in the track's order. A cancel ends the fetch early.
    async fn fetch_slices(&self, track: Address, track_data: &BlobEncoding, position: usize) -> Fetched {
        let profile = self.ctx.params().profile;
        let k = usize::from(profile.k());
        let group = VirtualGroup::containing(self.spool);
        let mut positions = ask_order(track, usize::from(profile.n()), position).into_iter();
        let mut fetched = Fetched { slices: Vec::with_capacity(k), rejected: 0 };
        let mut in_flight: JoinSet<(usize, Answer)> = JoinSet::new();

        loop {
            while in_flight.len() < RECOVER_FETCH_CONCURRENCY {
                let Some(position) = positions.next() else {
                    break;
                };

                let helper_spool = group.spool_at(position);
                let previous = self.peers.previous.get(&helper_spool).copied();
                let current = self.peers.current.get(&helper_spool).copied();
                let holders = [previous, current.filter(|node| previous != Some(*node))];
                let req = GetSliceReq { track, spool: helper_spool };
                let peer_manager = self.ctx.peer_manager.clone();
                let api = self.ctx.api.clone();
                let encoding = *track_data;

                in_flight.spawn(
                    async move {
                        let answer = fetch_position(peer_manager, api, holders, req, position, encoding).await;
                        (position, answer)
                    }
                    .in_current_span(),
                );
            }

            let joined = tokio::select! {
                _ = self.token.cancelled() => {
                    in_flight.abort_all();
                    while in_flight.join_next().await.is_some() {}
                    return fetched;
                }
                joined = in_flight.join_next() => joined,
            };
            let Some(joined) = joined else {
                return fetched;
            };

            match joined {
                Ok((position, answer)) => {
                    self.ctx.metrics.add_recover_fetched(answer.bytes);
                    fetched.rejected += answer.rejected;
                    if let Some(slice) = answer.slice {
                        fetched.slices.push((SliceIndex::new(position), slice));
                    }
                    if fetched.slices.len() >= k {
                        in_flight.abort_all();
                        while in_flight.join_next().await.is_some() {}
                        return fetched;
                    }
                }
                Err(error) => {
                    warn!(spool = %self.spool, track = %track, %error, "recovery fetch task failed");
                }
            }
        }
    }
}

/// The positions a try asks, in turn from a start the track's address picks, ours left out
///
/// Each track starts somewhere else, so the low positions don't carry every
/// try and a hung position isn't asked first on every track.
fn ask_order(track: Address, n: usize, ours: usize) -> Vec<usize> {
    let bytes = track.to_bytes();
    let mut head = [0u8; 8];
    head.copy_from_slice(&bytes[..8]);
    let start = (u64::from_be_bytes(head) % n.max(1) as u64) as usize;

    (0..n)
        .map(|step| (start + step) % n)
        .filter(|&position| position != ours)
        .collect()
}

/// Ask a position's holders in turn, each once, for a slice that matches its leaf
///
/// A holder gives up once its body moves no byte for TRANSFER_IDLE, so a slow
/// holder that keeps sending finishes.
async fn fetch_position<Cluster: Api + 'static>(
    peer_manager: Arc<PeerManager>,
    api: Arc<Cluster>,
    holders: [Option<Address>; 2],
    req: GetSliceReq,
    position: usize,
    track_data: BlobEncoding,
) -> Answer {
    let mut answer = Answer { slice: None, rejected: 0, bytes: 0 };
    for node in holders.into_iter().flatten() {
        let fetched = call_peer(&peer_manager, RetryConfig::none(), node, None, || {
            with_idle_limit(TRANSFER_IDLE, api.get_slice(node, &req))
        })
        .await;
        let Ok(res) = fetched else {
            continue;
        };
        if res.data.is_empty() {
            continue;
        }

        answer.bytes += res.data.len() as u64;
        if track_data.verify_slice(SpoolIndex::from(position as u64), &res.data) {
            answer.slice = Some(Vec::from(res.data));
            return answer;
        }
        answer.rejected += 1;
    }
    answer
}

/// Decode k peer slices back to the original blob, re-encode, extract our slice.
fn reconstruct(
    slicer: &mut Slicer<StripedCoder>,
    lost: SliceIndex,
    peer_slices: &[(SliceIndex, Vec<u8>)],
) -> Result<Vec<u8>, String> {
    let Some((_, sample)) = peer_slices.first() else {
        return Err("no peer slices provided".into());
    };

    let metadata = slicer
        .read_metadata(sample)
        .map_err(|error| format!("parse peer metadata failed: {error}"))?;

    slicer.set_chunk_index(metadata.chunk_index());

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
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, slice_root};
    use tape_core::spooler::VirtualGroup;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber,
        TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::ApiError;
    use tape_protocol::api::ops::{GetSliceRes, PeerReq, PeerRes};
    use tape_slicer::SliceMetadata;
    use tape_core::tape::TapeFlags;
    use tape_store::ops::{ObjectInfoOps, QueuedTrack};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);
    const TAPE_NUMBER: TapeNumber = TapeNumber(1);

    /// The slicer a tapenet node encodes and recovers with.
    fn tapenet_slicer() -> Slicer<StripedCoder> {
        Slicer::try_from_profile(TAPENET_PROFILE).unwrap()
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn lost() -> usize {
        VirtualGroup::containing(SPOOL).position_of(SPOOL).unwrap()
    }

    /// The previous holder `recover_state` lists for a position
    fn previous_holder(position: usize) -> Address {
        addr(200 + position as u8)
    }

    async fn test_context() -> TestContext {
        test_context_with_api(MemoryApi::noop()).await
    }

    // a node that holds the row of the tape every test track is on
    async fn test_context_with_api(api: MemoryApi) -> TestContext {
        let ctx = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize());

        let tape = TapeInfo::new(TAPE_NUMBER, TapeFlags::NONE, EpochNumber(u64::MAX), TrackNumber(1));
        ctx.store.put_tape(addr(0), tape).expect("put tape");
        ctx
    }

    fn clay_blob(size: u64, slices: &[Vec<u8>]) -> BlobEncoding {
        let metadata = SliceMetadata::parse(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let leaves = core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        BlobEncoding {
            size: StorageUnits::from_bytes(size),
            commitment,
            stripe_size: StorageUnits::from_bytes(stripe_size),
            stripe_count: StripeCount(size.div_ceil(stripe_size)),
            leaves,
        }
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let blob = clay_blob(size, slices);
        CompressedTrack {
            tape: addr(0),
            tape_number: TAPE_NUMBER,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size),
            group: VirtualGroup::containing(SPOOL),
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
            *helper = Some(previous_holder(slice));
        }
        state
    }

    /// Queue a certified track for recovery, with its track row, data and spool state
    fn queue_track(ctx: &TestContext, track: Address, slices: &[Vec<u8>]) {
        ctx.store.set_spool_state(SPOOL, recover_state(EpochNumber(3))).unwrap();
        ctx.store.put_track(track, clay_track(1024, slices)).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(clay_blob(1024, slices))).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();
    }

    /// Every slice ask, by position and holder
    type Asks = Arc<Mutex<Vec<(usize, Address)>>>;

    /// Holders that serve the encoded slices, unless `refuse` gives an error
    /// for the position and holder asked. Every ask is recorded.
    fn holders(
        slices: &[Vec<u8>],
        refuse: impl Fn(usize, Address) -> Option<ApiError> + Send + Sync + 'static,
    ) -> (MemoryApi, Asks) {
        let group = VirtualGroup::containing(SPOOL);
        let slices = slices.to_vec();
        let asked = Arc::new(Mutex::new(Vec::new()));
        let record = asked.clone();
        let api = MemoryApi::new(move |node, req| match req {
            PeerReq::GetSlice(ref r) => {
                let position = group.position_of(r.spool).unwrap();
                record.lock().unwrap().push((position, node));
                match refuse(position, node) {
                    Some(error) => PeerRes::GetSlice(Err(error)),
                    None => PeerRes::GetSlice(Ok(GetSliceRes { data: slices[position].clone().into() })),
                }
            }
            _ => panic!("unexpected request"),
        });
        (api, asked)
    }

    #[tokio::test]
    async fn empty_queue() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, recover_state(EpochNumber(3)))
            .unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
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
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    #[tokio::test]
    async fn full_recovery() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, _) = holders(&slices, |_, _| None);
        let ctx = test_context_with_api(api).await;
        let track = addr(1);
        queue_track(&ctx, track, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }

    // with no holder answering, the try fails and the row goes behind the queue
    #[tokio::test]
    async fn insufficient_peers() {
        let ctx = test_context().await; // noop api returns errors
        let a = addr(1);
        let slices = tapenet_slicer().encode(&vec![0x11; 1024]).unwrap();
        queue_track(&ctx, a, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 1 });
        let rows = ctx.store.recoveries_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(1), track: a }]);
    }

    #[tokio::test]
    async fn skips_uncertified() {
        let ctx = test_context().await;
        let a = addr(2);
        let slicer = tapenet_slicer();
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
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, a).unwrap());
    }

    /// Per-helper fallback can combine previous helpers from local spool state
    /// with current helpers from protocol state and recover successfully.
    #[tokio::test]
    async fn split_peers() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, _) = holders(&slices, |_, _| None);
        let ctx = test_context_with_api(api).await;
        let track = addr(1);
        queue_track(&ctx, track, &slices);

        // Previous: only positions 0..3 → 4 helpers (< k=7)
        let mut state = SpoolState::new(SpoolStatus::Recover, EpochNumber(3));
        for pos in 0..4 {
            state.prev_helpers[pos] = Some(previous_holder(pos));
        }
        ctx.store.set_spool_state(SPOOL, state).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
    }

    #[test]
    fn reconstruct_roundtrip() {
        let mut slicer = tapenet_slicer();
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

        let mut recovery_slicer = tapenet_slicer();
        let recovered = reconstruct(&mut recovery_slicer, lost, &peer_slices).unwrap();
        assert_eq!(recovered, slices[*lost]);
    }

    /// Only k helper positions return valid data; the rest return errors.
    /// Recovery should succeed without waiting for all positions.
    #[tokio::test]
    async fn early_stop() {
        let k = usize::from(TAPENET_PROFILE.k());
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let good: HashSet<usize> = (0..usize::from(TAPENET_PROFILE.n()))
            .filter(|&position| position != lost())
            .take(k)
            .collect();
        let (api, _) = holders(&slices, move |position, _| {
            (!good.contains(&position)).then(|| ApiError::Other("simulated failure".into()))
        });
        let ctx = test_context_with_api(api).await;
        let track = addr(1);
        queue_track(&ctx, track, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }

    // both holders of the first position asked serve a corrupt slice; the try
    // drops and counts both, and restores the track from other positions
    #[tokio::test]
    async fn corrupt_holder() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let track = addr(9);
        let n = usize::from(TAPENET_PROFILE.n());
        let corrupt = ask_order(track, n, lost())[0];
        let mut served = slices.clone();
        served[corrupt][0] ^= 0xFF;
        let (api, asked) = holders(&served, |_, _| None);
        let ctx = test_context_with_api(api).await;
        queue_track(&ctx, track, &slices);
        let token = CancellationToken::new();
        let state = recover_state(EpochNumber(3));
        let recovery_run = RecoveryRun {
            ctx: ctx.as_ref(),
            spool: SPOOL,
            peers: group_peers(ctx.as_ref(), &state, SPOOL),
            token: &token,
        };

        let fetched = recovery_run.fetch_slices(track, &clay_blob(1024, &slices), lost()).await;

        assert_eq!(fetched.rejected, 2);
        assert_eq!(fetched.slices.len(), usize::from(TAPENET_PROFILE.k()));
        assert!(fetched.slices.iter().all(|(position, _)| **position != corrupt));
        let asks = asked.lock().unwrap().iter().filter(|(position, _)| *position == corrupt).count();
        assert_eq!(asks, 2);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
    }

    // a try past a corrupt holder counts the rejected slices and the recovered try
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn recover_counters() {
        let set = tape_metrics::metrics();
        let recovered = || set.recover_tries_total.with_label_values(&["recovered"]).get();
        let before = (recovered(), set.recover_rejected_slices_total.get());
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let track = addr(9);
        let n = usize::from(TAPENET_PROFILE.n());
        let corrupt = ask_order(track, n, lost())[0];
        let mut served = slices.clone();
        served[corrupt][0] ^= 0xFF;
        let (api, _) = holders(&served, |_, _| None);
        let ctx = test_context_with_api(api).await;
        queue_track(&ctx, track, &slices);

        run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        // Other tests share the process's counters, so each only has to grow
        assert!(recovered() > before.0);
        assert!(set.recover_rejected_slices_total.get() >= before.1 + 2);
    }

    // a hung previous holder costs one idle limit, then its current holder
    // answers and the track is restored
    #[tokio::test(start_paused = true)]
    async fn hung_holder() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let track = addr(9);
        let n = usize::from(TAPENET_PROFILE.n());
        let k = usize::from(TAPENET_PROFILE.k());
        let order = ask_order(track, n, lost());
        let answering: HashSet<usize> = order[..k].iter().copied().collect();
        let hung = order[0];
        let group = VirtualGroup::containing(SPOOL);
        let (api, _) = holders(&slices, move |position, _| {
            (!answering.contains(&position)).then_some(ApiError::NotFound)
        });
        let api = api.hanging(move |node, req| match req {
            PeerReq::GetSlice(r) => group.position_of(r.spool) == Some(hung) && node == previous_holder(hung),
            _ => false,
        });
        let ctx = test_context_with_api(api).await;
        queue_track(&ctx, track, &slices);
        let started = tokio::time::Instant::now();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        let elapsed = started.elapsed();
        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert!(elapsed >= TRANSFER_IDLE, "the hung holder ended early: {elapsed:?}");
        assert!(elapsed < TRANSFER_IDLE + Duration::from_secs(1), "the try outlived one idle limit: {elapsed:?}");
    }

    // a failed try puts its row behind the queue, and the row behind it
    // still runs in the same pass
    #[tokio::test]
    async fn failed_to_back() {
        let failing = addr(1);
        let restored = addr(2);
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let group = VirtualGroup::containing(SPOOL);
        let served = slices.clone();
        let api = MemoryApi::new(move |_, req| match req {
            PeerReq::GetSlice(ref r) if r.track == restored => {
                let position = group.position_of(r.spool).unwrap();
                PeerRes::GetSlice(Ok(GetSliceRes { data: served[position].clone().into() }))
            }
            PeerReq::GetSlice(_) => PeerRes::GetSlice(Err(ApiError::NotFound)),
            _ => panic!("unexpected request"),
        });
        let ctx = test_context_with_api(api).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        queue_track(&ctx, failing, &slices);
        queue_track(&ctx, restored, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RecoverResult::Done { unrecovered: 1 });
        assert_eq!(ctx.store.get_slice(SPOOL, restored).unwrap().unwrap(), slices[lost()]);
        assert!(!ctx.store.has_pending_recovery(SPOOL, restored).unwrap());
        let rows = ctx.store.recoveries_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(11), track: failing }]);
    }

    // each track starts at a position its address picks, and every order
    // covers every position but ours once
    #[test]
    fn start_varies() {
        let n = usize::from(TAPENET_PROFILE.n());
        let mut starts = HashSet::new();

        for byte in 0..=u8::MAX {
            let order = ask_order(addr(byte), n, lost());
            let positions: HashSet<usize> = order.iter().copied().collect();
            assert_eq!(order.len(), n - 1);
            assert_eq!(positions.len(), n - 1);
            assert!(!positions.contains(&lost()));
            starts.insert(order[0]);
        }

        assert_eq!(starts.len(), n - 1, "every position but ours starts some track");
    }

    // a row recovery can't act on yet goes back, ends the run, and isn't due
    // again until the cursor moves
    #[tokio::test]
    async fn kept_row_waits() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, asked) = holders(&slices, |_, _| None);
        let ctx = test_context_with_api(api).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        let track = addr(9);
        ctx.store.set_spool_state(SPOOL, recover_state(EpochNumber(3))).unwrap();
        ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_recovery(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        assert!(asked.lock().unwrap().is_empty());
        assert!(ctx.store.recoveries_due(SPOOL, None, SlotNumber(10), 10).unwrap().is_empty());
        ctx.store.set_sync_cursor(SlotNumber(11), None).unwrap();
        let rows = ctx.store.recoveries_due(SPOOL, None, SlotNumber(11), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(11), track }]);
    }

    // a cancel isn't a try, so the row keeps its slot
    #[tokio::test(start_paused = true)]
    async fn cancel_keeps_row() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, _) = holders(&slices, |_, _| None);
        let ctx = test_context_with_api(api.hanging(|_, _| true)).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        let track = addr(9);
        queue_track(&ctx, track, &slices);
        let token = CancellationToken::new();
        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            canceller.cancel();
        });

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &token).await;

        assert_eq!(result, RecoverResult::Done { unrecovered: 0 });
        let rows = ctx.store.recoveries_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(10), track }]);
    }

    // the holders a try asks are the previous and current holders, each once
    #[tokio::test]
    async fn one_try_per_holder() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, asked) = holders(&slices, |_, _| Some(ApiError::Timeout));
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        queue_track(&ctx, track, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RecoverResult::Done { unrecovered: 1 });
        let mut tries: HashMap<(usize, Address), usize> = HashMap::new();
        for ask in asked.lock().unwrap().iter() {
            *tries.entry(*ask).or_default() += 1;
        }
        assert!(tries.values().all(|&count| count == 1), "a holder was asked twice: {tries:?}");
        let first = ask_order(track, usize::from(TAPENET_PROFILE.n()), lost())[0];
        assert!(tries.contains_key(&(first, previous_holder(first))));
    }
}
