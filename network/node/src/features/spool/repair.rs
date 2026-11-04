use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tape_peer_manager::PeerManager;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::VirtualGroup;
use tape_core::types::SpoolIndex;
use tape_core::system::SpoolState;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::types::{StorageUnits, StripeCount};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::ApiError;
use tape_protocol::api::ops::{RepairReq, RepairRes};
use tape_protocol::api::types::StripeSubChunkRequest;
use tape_retry::RetryConfig;
use tape_slicer::{
    ClayCoder, ErasureCoder, HelperData, HelperRequest, SliceIndex, SliceMetadata, Slicer,
};
use tape_store::ops::{
    MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps, track_has_left,
};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::types::RepairResult;

const REPAIR_FETCH_CONCURRENCY: usize = 4;

// Purpose: Bandwidth-optimal Clay repair for missing slices.
//          Drains the pending_repairs queue that Sync, Scan and the
//          certify hook fill.
//          Tracks that cannot be Clay-repaired are escalated to the
//          pending_recoveries queue for the Recover task.
//
// "Escalate" means: remove from pending_repairs, add to pending_recoveries.
// An add keeps a row the track already has, so adds are idempotent.
//
// Partial repair is a Clay feature. On a Reed-Solomon or copies network the
// whole queue is escalated, and the Recover task rebuilds from k slices.
//
// Algorithm:
// 1. Load spool state. Derive group and our slice index within it.
//    Build two peer maps from spool_state.prev_helpers (previous)
//    and protocol.group_peers(group) (current).
//    Exclude our own spool from both maps.
//
// 2. One pass over the rows due at the applied slot read when the run starts,
//    in queue order, store.repairs_due(spool, after, until, batch_size) from
//    the last row read. A row repair can't act on yet is put back
//    (requeue_repair) and waits for the cursor to move.
//
//    For each track_address:
//      a. Check cancellation.
//      b. Skip if slice already present (has_slice). Remove from pending_repairs.
//      c. Load track_info. If missing, remove from pending_repairs, continue.
//      d. One attempt (repair_track):
//         - Wave 1 plans every stripe from every position with a holder in
//           either map. A position with no holder is missing from the start.
//         - Each position gets one request, merged across its stripes, and
//           each of its holders gets one try within the request's deadline:
//           the previous holder, then the current one when it differs.
//         - A position whose holders all fail is missing for this attempt.
//           Every response that arrives is kept.
//         - Wave 2 plans the stripes wave 1 left short again, from the
//           positions that answered, then spares not yet asked, and asks
//           only for sub-chunks the attempt lacks. There is no third wave.
//         - Decode every stripe and check the slice against the track's leaf.
//
//      e. Repaired: store.put_slice, remove from pending_repairs.
//         Failed: escalate while the spool's recovery queue holds fewer
//         than recover_queue_limit rows, else put the row back.
//         Cancelled: the row stays as it was, and the run ends.
//
// 3. Return Done { unrepairable }, count of failed attempts.
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
) -> RepairResult {

    let spool_state = match ctx.store.get_spool_state(spool) {
        Ok(Some(state)) => state,
        _ => return RepairResult::Done { unrepairable: 0 },
    };

    let clay = match ctx.params().profile {
        ErasureCode::Clay(params) => params,
        ErasureCode::ReedSolomon(_) | ErasureCode::Replication(_) => {
            let unrepairable = escalate_queue(ctx.as_ref(), config, spool, token);
            return RepairResult::Done { unrepairable };
        }
    };

    let slicer = Slicer::with_rotation(ClayCoder::from_params(clay));
    let peers = group_peers(ctx.as_ref(), &spool_state, spool);

    let repair_run = RepairRun {
        ctx: ctx.as_ref(),
        config,
        slicer,
        spool,
        peers,
        token,
    };

    // The run takes the rows due when it starts. A row put back lands past
    // this bound, so it waits for the next run.
    let until = match ctx.store.get_sync_cursor() {
        Ok(cursor) => cursor.unwrap_or_default(),
        Err(error) => {
            warn!(spool = %spool, %error, "get_sync_cursor failed");
            return RepairResult::Done { unrepairable: 0 };
        }
    };

    let mut unrepairable = 0usize;
    let mut after = None;

    loop {
        if token.is_cancelled() {
            break;
        }

        let pending = match ctx
            .store
            .repairs_due(spool, after, until, config.repair_batch.max(1))
        {
            Ok(pending) => pending,
            Err(error) => {
                warn!(spool = %spool, %error, "repairs_due failed");
                break;
            }
        };

        if pending.is_empty() {
            break;
        }

        for row in pending {
            if token.is_cancelled() {
                break;
            }
            after = Some(row);
            let track = row.track;

            let has_slice = match ctx.store.has_slice(spool, track) {
                Ok(has_slice) => has_slice,
                Err(error) => {
                    warn!(spool = %spool, track = %track, %error, "has_slice failed");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
            };

            // If slice already exists, just remove from pending_repairs and skip.
            if has_slice {
                let _ = ctx.store.remove_pending_repair(spool, track);
                debug!(spool = %spool, track = %track, "slice already present, skipping");
                continue;
            }

            // Load track_info. If missing, remove from pending_repairs and skip.
            let track_info = match ctx.store.get_track(track) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    warn!(spool = %spool, track = %track, "track_info missing, removing");
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track, %error, "get_track failed");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
            };

            if !track_info.is_coded() {
                warn!(spool = %spool, track = %track, "non-blob track in repair queue, putting it back");
                put_back(ctx.as_ref(), spool, track);
                continue;
            }

            // A track that has left its tape is deleted by the sweep, so
            // nothing repairs it.
            match ctx.store.get_tape(track_info.tape) {
                Ok(tape) if track_has_left(tape.as_ref(), &track_info) => {
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    continue;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(spool = %spool, track = %track, %error, "get_tape failed");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
            }

            let track_data = match ctx.store.get_track_data(track) {
                Ok(Some(BlobData::Coded(info))) => info,
                Ok(Some(BlobData::Inline(_))) => {
                    warn!(spool = %spool, track = %track, "blob track has raw track_data, putting it back");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
                Ok(None) => {
                    warn!(spool = %spool, track = %track, "track_data missing, putting it back");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
                Err(error) => {
                    warn!(spool = %spool, track = %track, %error, "get_track_data failed");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
            };

            // Only repair certified tracks.
            match ctx.store.get_object_info(track) {
                Ok(Some(info)) if info.is_certified() => {}
                Ok(Some(_)) => {
                    let _ = ctx.store.remove_pending_repair(spool, track);
                    continue;
                }
                Ok(None) | Err(_) => {
                    warn!(spool = %spool, track = %track, "repair: state inconsistent or unreadable, putting it back");
                    put_back(ctx.as_ref(), spool, track);
                    continue;
                }
            }

            match repair_run.attempt(track, &track_data).await {
                Outcome::Repaired(data) => {
                    let repaired_len = data.len() as u64;
                    if let Err(error) = ctx.store.put_slice(spool, track, data) {
                        warn!(spool = %spool, track = %track, %error, "put_slice failed");
                        put_back(ctx.as_ref(), spool, track);
                        ctx.metrics.inc_repair_put_back();
                        continue;
                    }
                    ctx.metrics.add_repair_persisted(repaired_len);
                    ctx.metrics.inc_repair_repaired();
                    let _ = ctx.store.remove_pending_repair(spool, track);
                }
                Outcome::Failed => {
                    settle_failure(ctx.as_ref(), config, spool, track);
                    unrepairable += 1;
                }
                // An epoch turn or a shutdown isn't an attempt, so the row stays as it was
                Outcome::Cancelled => break,
            }
        }
    }

    RepairResult::Done { unrepairable }
}

/// Send a failed track to recovery while the spool's recovery queue has room,
/// or put it back behind the repair queue
fn settle_failure<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    track: Address,
) {
    let limit = config.recover_queue_limit;
    let queued = match ctx.store.iter_pending_recoveries(spool, limit) {
        Ok(rows) => rows.len(),
        Err(error) => {
            warn!(spool = %spool, track = %track, %error, "iter_pending_recoveries failed, putting it back");
            put_back(ctx, spool, track);
            ctx.metrics.inc_repair_put_back();
            return;
        }
    };

    if queued >= limit {
        info!(spool = %spool, track = %track, queued, "repair failed, recovery queue full, putting it back");
        put_back(ctx, spool, track);
        ctx.metrics.inc_repair_put_back();
        return;
    }

    info!(spool = %spool, track = %track, "repair failed, escalating to recovery");
    match ctx.store.add_pending_recovery(spool, track) {
        Ok(()) => {
            let _ = ctx.store.remove_pending_repair(spool, track);
            ctx.metrics.inc_repair_to_recovery();
        }
        Err(error) => {
            warn!(spool = %spool, track = %track, %error, "add_pending_recovery failed, putting it back");
            put_back(ctx, spool, track);
            ctx.metrics.inc_repair_put_back();
        }
    }
}

/// Put a row back behind the queue, where it waits for the cursor to move
fn put_back<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    track: Address,
) {
    if let Err(error) = ctx.store.requeue_repair(spool, track) {
        warn!(spool = %spool, track = %track, %error, "requeue_repair failed");
    }
}

/// Move a spool's whole repair queue to the recovery queue, and count what moved.
fn escalate_queue<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    token: &CancellationToken,
) -> usize {
    let mut escalated = 0usize;
    let until = match ctx.store.get_sync_cursor() {
        Ok(cursor) => cursor.unwrap_or_default(),
        Err(error) => {
            warn!(spool = %spool, %error, "get_sync_cursor failed");
            return escalated;
        }
    };
    let mut after = None;

    while !token.is_cancelled() {
        let pending = match ctx
            .store
            .repairs_due(spool, after, until, config.repair_batch.max(1))
        {
            Ok(pending) => pending,
            Err(error) => {
                warn!(spool = %spool, %error, "repairs_due failed");
                break;
            }
        };

        if pending.is_empty() {
            break;
        }

        for row in pending {
            after = Some(row);
            if let Err(error) = ctx.store.add_pending_recovery(spool, row.track) {
                warn!(spool = %spool, track = %row.track, %error, "add_pending_recovery failed, putting it back");
                put_back(ctx, spool, row.track);
                continue;
            }
            let _ = ctx.store.remove_pending_repair(spool, row.track);
            ctx.metrics.inc_repair_to_recovery();
            escalated += 1;
        }
    }

    escalated
}

/// Two peer maps: previous epoch helpers and current committee assignments.
pub struct GroupPeers {
    pub previous: HashMap<SpoolIndex, Address>,
    pub current: HashMap<SpoolIndex, Address>,
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
    let group = VirtualGroup::containing(spool);
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

/// How one repair attempt ended
enum Outcome {
    /// The slice, decoded and checked against the track's leaf
    Repaired(Vec<u8>),
    /// A stripe had no plan after wave 2, a decode errored, or the check failed
    Failed,
    /// An epoch turn or a shutdown stopped the attempt
    Cancelled,
}

/// Why a position is missing from an attempt
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MissingCause {
    /// Neither holder map lists the position
    NoHolder,
    /// The request ran past its deadline
    Deadline,
    /// The holder doesn't have the slice
    NotFound,
    /// The holder refused, or the connection failed
    Refused,
    /// The response didn't match the request
    WrongShape,
}

impl MissingCause {
    fn of(error: &ApiError) -> Self {
        match error {
            ApiError::Timeout => Self::Deadline,
            ApiError::NotFound => Self::NotFound,
            _ => Self::Refused,
        }
    }

    /// The cause's label on `tape_node_repair_missing_total`
    fn label(self) -> &'static str {
        match self {
            Self::NoHolder => "no_holder",
            Self::Deadline => "deadline",
            Self::NotFound => "not_found",
            Self::Refused => "refused",
            Self::WrongShape => "wrong_shape",
        }
    }
}

/// What one attempt holds, and how each position it asked fared
struct Fetched {
    held: HelperData,
    asked: BTreeSet<SliceIndex>,
    answered: BTreeSet<SliceIndex>,
    missing: BTreeMap<SliceIndex, MissingCause>,
}

/// One repair run over a stored spool. Its attempts share the context, the
/// slicer and the holders.
struct RepairRun<'a, Db: Store, Cluster: Api, Blockchain: Rpc> {
    ctx: &'a NodeContext<Db, Cluster, Blockchain>,
    config: &'a RecoveryConfig,
    slicer: Slicer<ClayCoder>,
    spool: SpoolIndex,
    peers: GroupPeers,
    token: &'a CancellationToken,
}

impl<Db: Store, Cluster: Api + 'static, Blockchain: Rpc> RepairRun<'_, Db, Cluster, Blockchain> {
    /// One attempt: wave 1, wave 2 for the stripes it left short, then the decode and the check
    async fn attempt(&self, track: Address, track_data: &BlobEncoding) -> Outcome {
        if track_data.stripe_size == StorageUnits::zero()
            || track_data.stripe_count == StripeCount::zero()
        {
            return Outcome::Failed;
        }

        let group = VirtualGroup::containing(self.spool);
        let Some(position) = group.position_of(self.spool) else {
            return Outcome::Failed;
        };
        let lost = SliceIndex::new(position);
        let blob_len = track_data.size.as_usize();
        let stripe_size = track_data.stripe_size.as_usize();

        // A position no holder map lists is missing from the start
        let mut holders = Vec::new();
        let mut missing = BTreeMap::new();
        for slice in (0..self.slicer.n()).map(SliceIndex::new) {
            if slice == lost {
                continue;
            }
            let helper_spool = group.spool_at(*slice);
            if self.peers.previous.contains_key(&helper_spool)
                || self.peers.current.contains_key(&helper_spool)
            {
                holders.push(slice);
            } else {
                missing.insert(slice, MissingCause::NoHolder);
            }
        }

        let mut plan = match self.slicer.repair_plan(lost, &holders, blob_len, stripe_size) {
            Ok(plan) => plan,
            Err(error) => {
                warn!(spool = %self.spool, track = %track, %error, "repair plan failed");
                return Outcome::Failed;
            }
        };
        let sub_chunk_size = StorageUnits::from_bytes(plan.sub_chunk_size);
        let mut fetched = Fetched {
            held: HelperData::new(&plan),
            asked: BTreeSet::new(),
            answered: BTreeSet::new(),
            missing,
        };

        let wave = plan.requests(&fetched.held);
        self.fetch_wave(track, wave, sub_chunk_size, &mut fetched).await;
        if self.token.is_cancelled() {
            return Outcome::Cancelled;
        }

        let short = plan.short_stripes(&fetched.held);
        if !short.is_empty() {
            let mut available: Vec<SliceIndex> = fetched.answered.iter().copied().collect();
            for slice in &holders {
                if !fetched.asked.contains(slice) {
                    available.push(*slice);
                }
            }
            if let Err(error) = self.slicer.replan(&mut plan, &available, &short) {
                warn!(spool = %self.spool, track = %track, %error, "repair replan failed");
                return Outcome::Failed;
            }

            let wave = plan.requests(&fetched.held);
            self.fetch_wave(track, wave, sub_chunk_size, &mut fetched).await;
            if self.token.is_cancelled() {
                return Outcome::Cancelled;
            }
        }

        for cause in fetched.missing.values() {
            self.ctx.metrics.inc_repair_missing(cause.label());
        }

        let metadata = SliceMetadata::new(blob_len, stripe_size).to_bytes();
        let repaired = match self.slicer.repair(&plan, &fetched.held, &metadata) {
            Ok(repaired) => repaired,
            Err(error) => {
                info!(
                    spool = %self.spool,
                    track = %track,
                    %error,
                    missing = ?fetched.missing,
                    "repair attempt failed"
                );
                return Outcome::Failed;
            }
        };

        if !track_data.verify_slice(SpoolIndex::from(position as u64), &repaired) {
            warn!(spool = %self.spool, track = %track, missing = ?fetched.missing, "repaired slice doesn't match the track's leaf");
            return Outcome::Failed;
        }

        // A stripe planned around a missing helper asks for more than d·β sub-chunks
        let base_cost = self.slicer.coder.d() * self.slicer.coder.beta();
        let mut around_missing = 0u64;
        for stripe in &plan.stripes {
            let Some(helpers) = &stripe.helpers else {
                continue;
            };
            let mut sub_chunks = 0usize;
            for helper in helpers {
                sub_chunks += helper.sub_chunks.len();
            }
            if sub_chunks > base_cost {
                around_missing += 1;
            }
        }
        self.ctx.metrics.add_repair_missing_helper_stripes(around_missing);

        Outcome::Repaired(repaired)
    }

    /// Ask each position for its request, up to REPAIR_FETCH_CONCURRENCY at
    /// once, and keep what arrives. A cancel ends the wave early.
    async fn fetch_wave(
        &self,
        track: Address,
        requests: Vec<HelperRequest>,
        sub_chunk_size: StorageUnits,
        fetched: &mut Fetched,
    ) {
        let group = VirtualGroup::containing(self.spool);
        let mut pending = requests.into_iter();
        let mut in_flight: JoinSet<(HelperRequest, Result<Vec<u8>, MissingCause>)> = JoinSet::new();

        loop {
            while in_flight.len() < REPAIR_FETCH_CONCURRENCY {
                let Some(request) = pending.next() else {
                    break;
                };
                fetched.asked.insert(request.slice);

                let helper_spool = group.spool_at(*request.slice);
                let previous = self.peers.previous.get(&helper_spool).copied();
                let current = self.peers.current.get(&helper_spool).copied();
                let holders = [previous, current.filter(|node| previous != Some(*node))];
                let req = repair_req(&request, helper_spool, track);
                let deadline = helper_deadline(self.config, &request, sub_chunk_size);
                let peer_manager = self.ctx.peer_manager.clone();
                let api = self.ctx.api.clone();

                in_flight.spawn(
                    async move {
                        let answer = fetch_position(peer_manager, api, holders, req, deadline).await;
                        (request, answer)
                    }
                    .in_current_span(),
                );
            }

            let joined = tokio::select! {
                _ = self.token.cancelled() => {
                    in_flight.abort_all();
                    while in_flight.join_next().await.is_some() {}
                    return;
                }
                joined = in_flight.join_next() => joined,
            };
            let Some(joined) = joined else {
                return;
            };

            match joined {
                Ok((request, Ok(data))) => {
                    self.ctx.metrics.add_repair_fetched(data.len() as u64);
                    match fetched.held.insert(&request, data) {
                        Ok(()) => {
                            fetched.answered.insert(request.slice);
                        }
                        Err(error) => {
                            debug!(spool = %self.spool, track = %track, %error, "repair helper answered the wrong shape");
                            fetched.missing.insert(request.slice, MissingCause::WrongShape);
                        }
                    }
                }
                Ok((request, Err(cause))) => {
                    fetched.missing.insert(request.slice, cause);
                }
                Err(error) => {
                    warn!(spool = %self.spool, track = %track, %error, "repair fetch task failed");
                }
            }
        }
    }
}

/// Ask a position's holders in turn, each once within the request's deadline
async fn fetch_position<Cluster: Api + 'static>(
    peer_manager: Arc<PeerManager>,
    api: Arc<Cluster>,
    holders: [Option<Address>; 2],
    req: RepairReq,
    deadline: Duration,
) -> Result<Vec<u8>, MissingCause> {
    let mut cause = MissingCause::NoHolder;
    for node in holders.into_iter().flatten() {
        let answer = call_peer(&peer_manager, RetryConfig::none(), node, None, || {
            repair_within(api.as_ref(), node, &req, deadline)
        })
        .await;
        match answer {
            Ok(res) => return Ok(res.data),
            Err(error) => cause = MissingCause::of(&error),
        }
    }
    Err(cause)
}

/// One repair call that gives up at its deadline, connect and body included
async fn repair_within<Cluster: Api>(
    api: &Cluster,
    node: Address,
    req: &RepairReq,
    deadline: Duration,
) -> Result<RepairRes, ApiError> {
    timeout(deadline, api.repair(node, req))
        .await
        .unwrap_or(Err(ApiError::Timeout))
}

/// A helper request's deadline: the base, plus its bytes at the floor rate
fn helper_deadline(
    config: &RecoveryConfig,
    request: &HelperRequest,
    sub_chunk_size: StorageUnits,
) -> Duration {
    let mut sub_chunks = 0u64;
    for stripe in &request.stripes {
        sub_chunks += stripe.sub_chunks.len() as u64;
    }
    let bytes = sub_chunks.saturating_mul(sub_chunk_size.to_bytes());
    let rate = config.repair_floor_rate.to_bytes().max(1);
    Duration::from_secs(config.repair_deadline_base_secs)
        + Duration::from_secs_f64(bytes as f64 / rate as f64)
}

/// The wire request for one position's sub-chunks
fn repair_req(request: &HelperRequest, helper_spool: SpoolIndex, track: Address) -> RepairReq {
    let mut stripes = Vec::with_capacity(request.stripes.len());
    for stripe in &request.stripes {
        stripes.push(StripeSubChunkRequest {
            stripe: stripe.stripe,
            sub_chunks: stripe.sub_chunks.clone(),
        });
    }
    RepairReq { track, helper_spool, stripes }
}


/// Extract sub-chunk data from a local slice to serve a repair request.
/// Called by the HTTP handler when a peer asks for repair data. Only a Clay
/// network has sub-chunks to serve.
pub fn extract_repair_data(
    profile: ErasureCode,
    stripes: &[StripeSubChunkRequest],
    slice_data: &[u8],
) -> Result<Vec<u8>, String> {
    let ErasureCode::Clay(clay) = profile else {
        return Err("partial repair needs a clay network".into());
    };

    let coder = ClayCoder::from_params(clay);
    let metadata = SliceMetadata::parse(slice_data)
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use peer_memory::MemoryApi;
    use tape_core::encoding::{RSParams, TAPENET_CLAY, TAPENET_PROFILE};
    use tape_core::erasure::{SLICE_TREE_HEIGHT, slice_root};
    use tape_core::system::SpoolStatus;
    use tape_core::thresholds::TrustModel;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::ops::{PeerReq, PeerRes};
    use tape_slicer::{slice_to_shard, MappingStrategy, StripedCoder};
    use tape_snapshot::encode_chunk;
    use tape_core::tape::TapeFlags;
    use tape_store::ops::{ObjectInfoOps, QueuedTrack};
    use tape_store::types::{ObjectInfo, TapeInfo};
    use tape_store::TapeStore;
    use tape_store_memory::MemoryStore;
    use tokio::time::timeout;

    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);
    const TAPE_NUMBER: TapeNumber = TapeNumber(1);

    /// The slicer a tapenet node encodes with.
    fn tapenet_slicer() -> Slicer<StripedCoder> {
        Slicer::try_from_profile(TAPENET_PROFILE).unwrap()
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
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
            *helper = Some(addr(100 + slice as u8));
        }
        state
    }

    // a run of rows it can't act on yet puts each one back past its bound once
    // and ends, a batch at a time, without taking any of them again
    #[tokio::test]
    async fn kept_rows_end_run() {
        let ctx = test_context().await;
        let slices = tapenet_slicer().encode(&vec![0x24; 1024]).unwrap();
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        let tracks = [addr(1), addr(2), addr(3)];
        for track in tracks {
            // Certified with no track data, so repair has nothing to plan from yet
            ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
            ctx.store.put_object_info(track, certified(track)).unwrap();
            ctx.store.add_pending_repair(SPOOL, track).unwrap();
        }
        let config = RecoveryConfig { repair_batch: 1, ..RecoveryConfig::default() };

        let ran = timeout(
            Duration::from_secs(10),
            run(ctx.clone(), &config, SPOOL, &CancellationToken::new()),
        )
        .await;

        assert_eq!(ran.expect("run ends"), RepairResult::Done { unrepairable: 0 });
        assert!(ctx.store.repairs_due(SPOOL, None, SlotNumber(10), 10).unwrap().is_empty());
        let put_back = ctx.store.repairs_due(SPOOL, None, SlotNumber(11), 10).unwrap();
        let held: Vec<Address> = put_back.iter().map(|row| row.track).collect();
        assert_eq!(held, tracks);
        assert!(put_back.iter().all(|row| row.ready == SlotNumber(11)));
    }

    // a row put back during a run waits for the next one, even when the cursor
    // moves while the run is still going
    #[tokio::test]
    async fn put_back_waits() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let group = VirtualGroup::containing(SPOOL);
        let slices_for_api = slices.clone();
        let store_cell: Arc<OnceLock<Arc<TapeStore<MemoryStore>>>> = Arc::default();
        let cell = store_cell.clone();

        // Each helper answer moves the cursor on, as the chain would during a long run
        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                if let Some(store) = cell.get() {
                    store.set_sync_cursor(SlotNumber(20), None).expect("move cursor");
                }
                let helper_slice = &slices_for_api[group.position_of(req.helper_spool).unwrap()];
                let data = extract_repair_data(TAPENET_PROFILE, &req.stripes, helper_slice).unwrap();
                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;
        store_cell.set(ctx.store.clone()).ok();

        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        let (kept, repaired) = (addr(1), addr(2));
        for track in [kept, repaired] {
            ctx.store.put_track(track, clay_track(1024, &slices)).unwrap();
            ctx.store.put_object_info(track, certified(track)).unwrap();
            ctx.store.add_pending_repair(SPOOL, track).unwrap();
        }
        ctx.store
            .put_track_data(repaired, BlobData::Coded(clay_blob(1024, &slices)))
            .unwrap();
        let config = RecoveryConfig { repair_batch: 1, ..RecoveryConfig::default() };

        let result = run(ctx.clone(), &config, SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(ctx.store.get_slice(SPOOL, repaired).unwrap().is_some());
        // Put back once, at the slot after the cursor the run started on
        let rows = ctx.store.repairs_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(11), track: kept }]);
    }

    #[tokio::test]
    async fn empty_queue() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();

        let result = run(ctx, &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
    }

    #[tokio::test]
    async fn skip_present() {
        let ctx = test_context().await;
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
        let slicer = tapenet_slicer();
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let track = addr(9);
        let group = VirtualGroup::containing(SPOOL);
        let lost_pos = group.position_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();
        let track_info = clay_track(1024, &slices);
        let track_blob = clay_blob(1024, &slices);
        let slices_for_api = slices.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                let helper_slice = &slices_for_api[group.position_of(req.helper_spool).unwrap()];

                let data = extract_repair_data(
                    TAPENET_PROFILE,
                    &req.stripes,
                    helper_slice,
                ).unwrap();

                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    #[tokio::test]
    async fn escalates_failure() {
        let ctx = test_context().await; // noop api, no peers
        let a = addr(1);
        let slicer = tapenet_slicer();
        let slices = slicer.encode(&vec![0x24; 1024]).unwrap();
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.put_track(a, clay_track(1024, &slices)).unwrap();
        ctx.store.put_track_data(a, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(a, certified(a)).unwrap();
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, a).unwrap());
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    // a reed-solomon network has no partial repair, so a queued repair goes
    // to recovery without asking any peer
    #[tokio::test]
    async fn no_partial_repair() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let ctx: TestContext = NodeHarness::builder()
            .network(profile, TrustModel::Trusted)
            .nodes(6)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);
        let spool = SpoolIndex(1);
        let track = addr(7);

        ctx.store
            .set_spool_state(spool, repair_state(EpochNumber(3)))
            .unwrap();
        ctx.store.add_pending_repair(spool, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), spool, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        assert!(ctx.store.has_pending_recovery(spool, track).unwrap());
        assert!(!ctx.store.has_pending_repair(spool, track).unwrap());
    }

    #[tokio::test]
    async fn missing_track() {
        let ctx = test_context().await;
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
        let ctx = test_context().await;
        let a = addr(2);
        let slicer = tapenet_slicer();
        let slices = slicer.encode(&vec![0x33; 1024]).unwrap();
        let track_blob = clay_blob(1024, &slices);

        ctx.store
            .set_spool_state(SPOOL, repair_state(EpochNumber(3)))
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
        ctx.store.add_pending_repair(SPOOL, a).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_repair(SPOOL, a).unwrap());
    }

    /// Per-helper fallback can combine previous helpers from local spool state
    /// with current helpers from protocol state and complete the repair.
    #[tokio::test]
    async fn split_peers() {
        let slicer = tapenet_slicer();
        let payload = vec![0x42u8; 1024];
        let slices = slicer.encode(&payload).unwrap();
        let track = addr(9);
        let group = VirtualGroup::containing(SPOOL);
        let lost_pos = group.position_of(SPOOL).unwrap();
        let expected = slices[lost_pos].clone();
        let track_info = clay_track(1024, &slices);
        let track_blob = clay_blob(1024, &slices);
        let slices_for_api = slices.clone();

        let ctx = test_context_with_api(MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(ref req) => {
                let helper_slice = &slices_for_api[group.position_of(req.helper_spool).unwrap()];
                let data = extract_repair_data(
                    TAPENET_PROFILE,
                    &req.stripes,
                    helper_slice,
                ).unwrap();
                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        }))
        .await;

        // Previous: positions 0..9 (excluding 5) → 9 helpers
        let mut state = SpoolState::new(SpoolStatus::Repair, EpochNumber(3));
        for pos in 0..10 {
            if pos != 5 {
                state.prev_helpers[pos] = Some(addr(100 + pos as u8));
            }
        }

        ctx.store.set_spool_state(SPOOL, state).unwrap();
        ctx.store.put_track(track, track_info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(track_blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), expected);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    /// The lost position: this node's place in the group
    fn lost() -> usize {
        VirtualGroup::containing(SPOOL).position_of(SPOOL).unwrap()
    }

    /// Whether a position's shard sits in the lost shard's y-section in a stripe
    fn same_section(stripe: usize, position: usize) -> bool {
        let coder = ClayCoder::from_params(TAPENET_CLAY);
        let q = coder.d() - coder.k() + 1;
        let n = coder.n();
        let shard = |slice| slice_to_shard(MappingStrategy::Rotated, n, stripe, slice);
        shard(position) / q == shard(lost()) / q
    }

    /// The other positions in stripe 0, same-section helpers first, each group ascending
    fn sections() -> (Vec<usize>, Vec<usize>) {
        let others = (0..usize::from(TAPENET_CLAY.n())).filter(|&position| position != lost());
        others.partition(|&position| same_section(0, position))
    }

    /// A rotated tapenet track of `stripes` stripes, one encode unit each
    fn rotated_track(stripes: usize) -> (u64, Vec<Vec<u8>>) {
        let mut slicer = tapenet_slicer();
        let unit = slicer.stripe_alignment();
        slicer.set_stripe_cap(unit);
        let len = stripes * unit;
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        (len as u64, slicer.encode(&payload).unwrap())
    }

    /// Helpers that serve `slices`, fail the positions `fails` picks, and log
    /// the position of every call
    fn helpers(
        slices: &[Vec<u8>],
        fails: impl Fn(usize) -> Option<ApiError> + Send + Sync + 'static,
    ) -> (MemoryApi, Arc<Mutex<Vec<usize>>>) {
        let slices = slices.to_vec();
        let group = VirtualGroup::containing(SPOOL);
        let asked: Arc<Mutex<Vec<usize>>> = Arc::default();
        let log = asked.clone();
        let api = MemoryApi::new(move |_, req| match req {
            PeerReq::Repair(req) => {
                let position = group.position_of(req.helper_spool).unwrap();
                log.lock().unwrap().push(position);
                if let Some(error) = fails(position) {
                    return PeerRes::Repair(Err(error));
                }
                let data = extract_repair_data(TAPENET_PROFILE, &req.stripes, &slices[position]).unwrap();
                PeerRes::Repair(Ok(RepairRes { data }))
            }
            _ => panic!("unexpected request"),
        });
        (api, asked)
    }

    /// Queue a certified track for repair on a Repair spool whose helpers all have holders
    fn queue_track(ctx: &TestContext, track: Address, size: u64, slices: &[Vec<u8>]) {
        ctx.store.set_spool_state(SPOOL, repair_state(EpochNumber(3))).unwrap();
        ctx.store.put_track(track, clay_track(size, slices)).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(clay_blob(size, slices))).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();
    }

    fn asked_count(asked: &Mutex<Vec<usize>>, position: usize) -> usize {
        asked.lock().unwrap().iter().filter(|&&asked| asked == position).count()
    }

    // a same-section helper answers NotFound, so wave 2 repairs its stripes
    // with missing-helper repair from the positions that answered
    #[tokio::test]
    async fn missing_same_section() {
        let (size, slices) = rotated_track(8);
        let (same, _) = sections();
        let down = same[0];
        let (api, asked) = helpers(&slices, move |position| (position == down).then_some(ApiError::NotFound));
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        queue_track(&ctx, track, size, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
        // Both holders once in wave 1, and wave 2 plans around it
        assert_eq!(asked_count(&asked, down), 2);
    }

    // a snapshot chunk from lib/snapshot's encoder repairs around a missing
    // same-section helper and matches its leaf
    #[tokio::test]
    async fn snapshot_chunk() {
        let symbol: Vec<u8> = (0..3_000).map(|i| (i % 251) as u8).collect();
        let group = VirtualGroup::containing(SPOOL);
        let built = encode_chunk(EpochNumber(5), group, ChunkNumber(0), &symbol, TAPENET_PROFILE).unwrap();
        let (same, _) = sections();
        let down = same[0];
        let (api, _) = helpers(&built.slices, move |position| (position == down).then_some(ApiError::NotFound));
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        let mut info = clay_track(built.blob.size.to_bytes(), &built.slices);
        info.value_hash = built.blob.get_hash();
        ctx.store.set_spool_state(SPOOL, repair_state(EpochNumber(3))).unwrap();
        ctx.store.put_track(track, info).unwrap();
        ctx.store.put_track_data(track, BlobData::Coded(built.blob)).unwrap();
        ctx.store.put_object_info(track, certified(track)).unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        let repaired = ctx.store.get_slice(SPOOL, track).unwrap().unwrap();
        assert_eq!(repaired, built.slices[lost()]);
        assert!(built.blob.verify_slice(SpoolIndex::from(lost() as u64), &repaired));
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
    }

    // a repair around a missing same-section helper counts the attempt, the
    // missing position and the stripes planned around it
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn repair_counters() {
        let set = tape_metrics::metrics();
        let repaired = || set.repair_attempts_total.with_label_values(&["repaired"]).get();
        let not_found = || set.repair_missing_total.with_label_values(&["not_found"]).get();
        let before = (
            repaired(),
            not_found(),
            set.repair_missing_helper_stripes_total.get(),
        );
        let (size, slices) = rotated_track(8);
        let (same, _) = sections();
        let down = same[0];
        let (api, _) = helpers(&slices, move |position| (position == down).then_some(ApiError::NotFound));
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        queue_track(&ctx, track, size, &slices);

        run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        // Other tests share the process's counters, so each only has to grow
        assert!(repaired() > before.0);
        assert!(not_found() > before.1);
        assert!(set.repair_missing_helper_stripes_total.get() > before.2);
    }

    // a helper whose holders both hang costs each one deadline, and the
    // attempt still repairs the track from a spare
    #[tokio::test(start_paused = true)]
    async fn hung_helper() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (_, other) = sections();
        let hung = other[0];
        let group = VirtualGroup::containing(SPOOL);
        let (api, _) = helpers(&slices, |_| None);
        let api = api.hanging(move |_, req| match req {
            PeerReq::Repair(req) => group.position_of(req.helper_spool) == Some(hung),
            _ => false,
        });
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);
        let config = RecoveryConfig::default();
        let started = tokio::time::Instant::now();

        let result = run(ctx.clone(), &config, SPOOL, &CancellationToken::new()).await;

        let elapsed = started.elapsed();
        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        let deadline = Duration::from_secs(config.repair_deadline_base_secs);
        assert!(elapsed >= deadline * 2, "two holders, one deadline each: {elapsed:?}");
        assert!(elapsed < deadline * 2 + Duration::from_secs(1), "the attempt outlived its deadlines: {elapsed:?}");
    }

    // two other-section helpers refuse, and wave 2 asks the first two spares
    #[tokio::test]
    async fn spares_fill_in() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (_, other) = sections();
        let k = usize::from(TAPENET_CLAY.k());
        let refused = [other[0], other[1]];
        let (api, asked) = helpers(&slices, move |position| {
            refused.contains(&position).then(|| ApiError::ConnectionFailed("refused".into()))
        });
        let ctx = test_context_with_api(api).await;
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert_eq!(asked_count(&asked, other[k]), 1);
        assert_eq!(asked_count(&asked, other[k + 1]), 1);
        assert_eq!(asked_count(&asked, other[k + 2]), 0);
    }

    /// Helpers where only `answering` positions answer, the rest NotFound
    fn only(slices: &[Vec<u8>], answering: Vec<usize>) -> MemoryApi {
        helpers(slices, move |position| (!answering.contains(&position)).then_some(ApiError::NotFound)).0
    }

    /// k - 1 positions that answer, all same-section helpers
    fn below_k() -> Vec<usize> {
        let (same, _) = sections();
        same[..usize::from(TAPENET_CLAY.k()) - 1].to_vec()
    }

    // fewer than k positions answer while the recovery queue has room, so
    // the track goes to recovery
    #[tokio::test]
    async fn to_recovery() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let ctx = test_context_with_api(only(&slices, below_k())).await;
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        assert!(ctx.store.has_pending_recovery(SPOOL, track).unwrap());
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    // the same with the recovery queue full, so the row goes behind the
    // repair queue at the slot after the applied slot
    #[tokio::test]
    async fn recovery_full() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let ctx = test_context_with_api(only(&slices, below_k())).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);
        ctx.store.add_pending_recovery(SPOOL, addr(8)).unwrap();
        let config = RecoveryConfig { recover_queue_limit: 1, ..RecoveryConfig::default() };

        let result = run(ctx.clone(), &config, SPOOL, &CancellationToken::new()).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 1 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
        let rows = ctx.store.repairs_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(11), track }]);
    }

    // a track put back is repaired on its next attempt, once its helpers answer
    #[tokio::test]
    async fn repaired_next_attempt() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let is_down = Arc::new(AtomicBool::new(true));
        let down = is_down.clone();
        let (api, _) = helpers(&slices, move |_| down.load(Ordering::Relaxed).then_some(ApiError::Timeout));
        let ctx = test_context_with_api(api).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);
        ctx.store.add_pending_recovery(SPOOL, addr(8)).unwrap();
        let config = RecoveryConfig { recover_queue_limit: 1, ..RecoveryConfig::default() };

        let first = run(ctx.clone(), &config, SPOOL, &CancellationToken::new()).await;
        assert_eq!(first, RepairResult::Done { unrepairable: 1 });
        assert!(ctx.store.has_pending_repair(SPOOL, track).unwrap());

        is_down.store(false, Ordering::Relaxed);
        ctx.store.set_sync_cursor(SlotNumber(11), None).unwrap();
        let second = run(ctx.clone(), &config, SPOOL, &CancellationToken::new()).await;

        assert_eq!(second, RepairResult::Done { unrepairable: 0 });
        assert_eq!(ctx.store.get_slice(SPOOL, track).unwrap().unwrap(), slices[lost()]);
        assert!(!ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    // a cancel during an attempt isn't an attempt: the row keeps its slot and
    // nothing goes to recovery
    #[tokio::test(start_paused = true)]
    async fn cancel_keeps_row() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (api, _) = helpers(&slices, |_| None);
        let ctx = test_context_with_api(api.hanging(|_, _| true)).await;
        ctx.store.set_sync_cursor(SlotNumber(10), None).unwrap();
        let track = addr(9);
        queue_track(&ctx, track, 1024, &slices);
        let token = CancellationToken::new();
        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            canceller.cancel();
        });

        let result = run(ctx.clone(), &RecoveryConfig::default(), SPOOL, &token).await;

        assert_eq!(result, RepairResult::Done { unrepairable: 0 });
        assert!(!ctx.store.has_pending_recovery(SPOOL, track).unwrap());
        let rows = ctx.store.repairs_due(SPOOL, None, SlotNumber(u64::MAX), 10).unwrap();
        assert_eq!(rows, [QueuedTrack { ready: SlotNumber(10), track }]);
    }

    // a position no holder map lists is planned around from the start and never asked
    #[tokio::test]
    async fn no_holder() {
        let slices = tapenet_slicer().encode(&vec![0x42u8; 1024]).unwrap();
        let (same, _) = sections();
        let absent = same[0];
        let (api, asked) = helpers(&slices, |_| None);
        let ctx = test_context_with_api(api).await;
        let group = VirtualGroup::containing(SPOOL);
        let mut previous = HashMap::new();
        for position in 0..usize::from(TAPENET_CLAY.n()) {
            if position != lost() && position != absent {
                previous.insert(group.spool_at(position), addr(100 + position as u8));
            }
        }
        let config = RecoveryConfig::default();
        let token = CancellationToken::new();
        let repair_run = RepairRun {
            ctx: ctx.as_ref(),
            config: &config,
            slicer: Slicer::with_rotation(ClayCoder::from_params(TAPENET_CLAY)),
            spool: SPOOL,
            peers: GroupPeers { previous, current: HashMap::new() },
            token: &token,
        };

        let outcome = repair_run.attempt(addr(9), &clay_blob(1024, &slices)).await;

        let Outcome::Repaired(repaired) = outcome else {
            panic!("the attempt didn't repair the track");
        };
        assert_eq!(repaired, slices[lost()]);
        assert_eq!(asked_count(&asked, absent), 0);
    }
}
