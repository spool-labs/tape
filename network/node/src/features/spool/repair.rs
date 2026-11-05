//! Repair: our slice rebuilt with Clay's bandwidth-optimal flow, each
//! helper sending part of its own slice
//!
//! Build the requests, send them, check what came back, write it, report.
//! A repair request carries one track, so a repair fetch holds one.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tape_core::encoding::ErasureCode;
use tape_core::erasure::leaf_position;
use tape_core::track::blob::BlobEncoding;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_protocol::api::{RepairReq, RepairRes, StripeSubChunkRequest};
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_slicer::extract_repair_data as extract_sub_chunks;
use tape_slicer::{
    ClayCoder, HelperData, HelperRequest, RepairPlan, SliceIndex,
    SliceMetadata, Slicer, StripeSubChunks,
};
use tape_store::types::AttemptResult;
use tape_store_api::Store;
use tokio::task::spawn_blocking;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::attempt::PlanRepair;
use crate::features::spool::fetch::{
    failed_track, send, Answer, CallFailure, Fetch, FetchReport, FetchTrack,
    TrackOutcome,
};
use crate::features::spool::peers::PeerOutcome;
use crate::features::spool::transfer::{is_slice_of, write_slice};

/// The slicer a Clay network repairs with
pub type ClaySlicer = Arc<Slicer<ClayCoder>>;

/// Plans a repair of one track's slice at one position
pub struct RepairPlanner {
    slicer: ClaySlicer,
    lost: SliceIndex,
    blob_len: usize,
    stripe_size: usize,
}

/// A repair plan with the requests it sends
struct PlannedRepair {
    plan: RepairPlan,
    requests: Vec<HelperRequest>,
}

/// What one helper's answer gave the repair
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HelperOutcome {
    Sent(StorageUnits),
    Wrong,
    NotHeld,
    Failed(PeerOutcome),
}

/// How a repair's helpers did, for the track
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Helped {
    is_wrong: bool,
    is_not_held: bool,
    failure: Option<PeerOutcome>,
    received: StorageUnits,
}

/// The slicer to repair with, on a Clay network
pub fn clay_slicer(profile: ErasureCode) -> Option<ClaySlicer> {
    let ErasureCode::Clay(params) = profile else {
        return None;
    };
    let coder = ClayCoder::from_params(params);
    Some(Arc::new(Slicer::with_rotation(coder)))
}

impl RepairPlanner {
    /// A planner for the slice at `position` of the track `blob` encodes
    pub fn new(
        slicer: ClaySlicer,
        position: usize,
        blob: &BlobEncoding,
    ) -> Self {
        Self {
            slicer,
            lost: SliceIndex::new(position),
            blob_len: blob.size.as_usize(),
            stripe_size: blob.stripe_size.as_usize(),
        }
    }

    /// A plan from `available`, in order of preference, with every stripe
    /// planned
    fn full_plan(&self, available: &[usize]) -> Option<RepairPlan> {
        let mut slices = Vec::with_capacity(available.len());
        for position in available {
            slices.push(SliceIndex::new(*position));
        }
        let (blob_len, stripe_size) = (self.blob_len, self.stripe_size);
        let planned =
            self.slicer.repair_plan(self.lost, &slices, blob_len, stripe_size);
        let plan = planned.ok()?;
        for stripe in &plan.stripes {
            stripe.helpers.as_ref()?;
        }
        Some(plan)
    }
}

impl PlanRepair for RepairPlanner {
    fn helpers(&self) -> usize {
        self.slicer.coder.d()
    }

    fn plan(&self, available: &[usize]) -> Option<Vec<usize>> {
        let plan = self.full_plan(available)?;
        let mut positions = Vec::new();
        for request in plan.requests(&HelperData::new(&plan)) {
            positions.push(*request.slice);
        }
        Some(positions)
    }
}

/// Run one repair fetch for its one track
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    fetch: Fetch,
    limit: Duration,
    slicer: ClaySlicer,
) -> FetchReport {
    let mut report = FetchReport::new(fetch);
    let Some(track) = report.fetch.tracks.first().cloned() else {
        return report;
    };
    let planner = Arc::clone(&slicer);
    let Some(planned) = plan_fetch(&report.fetch, &track, planner) else {
        let not_held = AttemptResult::NotHeld;
        report.every_track(TrackOutcome::Failed(not_held));
        return report;
    };

    let fetch = &report.fetch;
    let answers = ask_helpers(&ctx, fetch, &track, &planned, limit).await;
    let mut held = HelperData::new(&planned.plan);
    let helped = gather(&ctx, &mut report, &planned, &mut held, answers);
    ctx.metrics.add_repair_fetched(helped.received.as_u64());

    let outcome = match helped_outcome(helped) {
        Some(outcome) => outcome,
        None => {
            let fetch = &report.fetch;
            let plan = planned.plan;
            rebuild(&ctx, fetch, &track, slicer, plan, held).await
        }
    };
    count(&ctx, outcome);
    report.track(track.key, outcome);
    report
}

fn plan_fetch(
    fetch: &Fetch,
    track: &FetchTrack,
    slicer: ClaySlicer,
) -> Option<PlannedRepair> {
    let blob = track.blob.as_ref()?;
    let position = leaf_position(fetch.spool).as_usize();
    let planner = RepairPlanner::new(slicer, position, blob);
    let mut available = Vec::with_capacity(fetch.peers.len());
    for helper in &fetch.peers {
        available.push(helper.position);
    }
    let plan = planner.full_plan(&available)?;
    let requests = plan.requests(&HelperData::new(&plan));
    Some(PlannedRepair { plan, requests })
}

/// Send each helper its request at once
async fn ask_helpers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    fetch: &Fetch,
    track: &FetchTrack,
    planned: &PlannedRepair,
    limit: Duration,
) -> Vec<Option<Answer<RepairRes>>> {
    let mut requests = Vec::with_capacity(planned.requests.len());
    for request in &planned.requests {
        requests.push(wire_request(track, request));
    }

    let mut calls = Vec::with_capacity(requests.len());
    for (planned, request) in planned.requests.iter().zip(&requests) {
        let peer = helper_peer(fetch, *planned.slice);
        calls.push(ask_one(ctx, peer, request, limit));
    }
    join_all(calls).await
}

async fn ask_one<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    peer: Option<Address>,
    request: &RepairReq,
    limit: Duration,
) -> Option<Answer<RepairRes>> {
    let call = ctx.api.repair(peer?, request);
    Some(send(limit, call).await)
}

fn helper_peer(fetch: &Fetch, position: usize) -> Option<Address> {
    for helper in &fetch.peers {
        if helper.position == position {
            return Some(helper.peer);
        }
    }
    None
}

fn wire_request(track: &FetchTrack, request: &HelperRequest) -> RepairReq {
    RepairReq {
        track: track.key.track,
        helper_spool: track.track.group.spool_at(*request.slice),
        stripes: wire_stripes(&request.stripes),
    }
}

fn wire_stripes(stripes: &[StripeSubChunks]) -> Vec<StripeSubChunkRequest> {
    let mut wire = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        let sub_chunks = stripe.sub_chunks.clone();
        let stripe = stripe.stripe;
        wire.push(StripeSubChunkRequest { stripe, sub_chunks });
    }
    wire
}

/// Hold what each helper sent and record how it did
fn gather<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    report: &mut FetchReport,
    planned: &PlannedRepair,
    held: &mut HelperData,
    answers: Vec<Option<Answer<RepairRes>>>,
) -> Helped {
    let mut helped = Helped::default();
    for (request, answer) in planned.requests.iter().zip(answers) {
        let peer = helper_peer(&report.fetch, *request.slice);
        let (Some(peer), Some(answer)) = (peer, answer) else {
            continue;
        };
        let outcome = gather_one(held, request, answer.response);
        report.peer(peer, peer_outcome(outcome, answer.elapsed));
        add_helper(&mut helped, outcome);
        count_missing(ctx, outcome);
    }
    helped
}

fn gather_one(
    held: &mut HelperData,
    request: &HelperRequest,
    response: Result<RepairRes, CallFailure>,
) -> HelperOutcome {
    let response = match response {
        Ok(response) => response,
        Err(CallFailure::NotHeld) => return HelperOutcome::NotHeld,
        Err(CallFailure::Peer(outcome)) => {
            return HelperOutcome::Failed(outcome);
        }
    };
    let size = StorageUnits(response.data.len() as u64);
    match held.insert(request, response.data) {
        Ok(()) => HelperOutcome::Sent(size),
        Err(_) => HelperOutcome::Wrong,
    }
}

fn peer_outcome(outcome: HelperOutcome, elapsed: Duration) -> PeerOutcome {
    match outcome {
        HelperOutcome::Sent(bytes) => PeerOutcome::Worked { bytes, elapsed },
        HelperOutcome::Wrong => PeerOutcome::WrongData,
        HelperOutcome::NotHeld => {
            let bytes = StorageUnits(0);
            PeerOutcome::Worked { bytes, elapsed }
        }
        HelperOutcome::Failed(outcome) => outcome,
    }
}

fn add_helper(helped: &mut Helped, outcome: HelperOutcome) {
    match outcome {
        HelperOutcome::Sent(bytes) => {
            helped.received = helped.received.saturating_add(bytes);
        }
        HelperOutcome::Wrong => helped.is_wrong = true,
        HelperOutcome::NotHeld => helped.is_not_held = true,
        HelperOutcome::Failed(outcome) => helped.failure = Some(outcome),
    }
}

/// The track's outcome when a helper fell short, or nothing to rebuild on
fn helped_outcome(helped: Helped) -> Option<TrackOutcome> {
    if helped.is_wrong {
        return Some(TrackOutcome::Failed(AttemptResult::WrongData));
    }
    if helped.is_not_held {
        return Some(TrackOutcome::Failed(AttemptResult::NotHeld));
    }
    helped.failure.map(failed_track)
}

/// Rebuild our slice from what the helpers sent, check it, and write it
async fn rebuild<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    fetch: &Fetch,
    track: &FetchTrack,
    slicer: ClaySlicer,
    plan: RepairPlan,
    held: HelperData,
) -> TrackOutcome {
    let Some(blob) = track.blob else {
        return wrong(track, "no encoding".to_string());
    };
    let size = blob.size.as_usize();
    let metadata = SliceMetadata::new(size, blob.stripe_size.as_usize());
    let bytes = metadata.to_bytes();
    let repair = move || slicer.repair(&plan, &held, &bytes);
    let rebuilt = match spawn_blocking(repair).await {
        Ok(Ok(slice)) => slice,
        Ok(Err(error)) => return wrong(track, error.to_string()),
        Err(error) => return wrong(track, error.to_string()),
    };

    let ours = leaf_position(fetch.spool);
    if !is_slice_of(track, ours, &rebuilt) {
        return wrong(track, "repaired slice fails its leaf".to_string());
    }
    let size = rebuilt.len() as u64;
    let key = track.key;
    if let Err(error) = write_slice(ctx, key.spool, key.track, rebuilt) {
        return wrong(track, error.to_string());
    }
    ctx.metrics.add_repair_persisted(size);
    TrackOutcome::Got
}

fn wrong(track: &FetchTrack, error: String) -> TrackOutcome {
    warn!(track = %track.key.track, %error, "spool: repair failed");
    TrackOutcome::Failed(AttemptResult::WrongData)
}

fn count<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    outcome: TrackOutcome,
) {
    match outcome {
        TrackOutcome::Got => ctx.metrics.inc_repair_repaired(),
        TrackOutcome::Failed(_) => ctx.metrics.inc_repair_put_back(),
    }
}

fn count_missing<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    outcome: HelperOutcome,
) {
    let cause = match outcome {
        HelperOutcome::Sent(_) => return,
        HelperOutcome::Wrong => "wrong_data",
        HelperOutcome::NotHeld => "not_held",
        HelperOutcome::Failed(PeerOutcome::TooSlow) => "deadline",
        HelperOutcome::Failed(PeerOutcome::Down) => "down",
        HelperOutcome::Failed(
            PeerOutcome::Refused { .. }
            | PeerOutcome::WrongData
            | PeerOutcome::Worked { .. },
        ) => "refused",
    };
    ctx.metrics.inc_repair_missing(cause);
}

/// Cut the sub-chunks a repair request asks for out of a helper's slice
pub fn extract_repair_data(
    profile: ErasureCode,
    stripes: &[StripeSubChunkRequest],
    slice_data: &[u8],
) -> Result<Vec<u8>, String> {
    let ErasureCode::Clay(params) = profile else {
        return Err("repair needs a clay network".to_string());
    };
    let coder = ClayCoder::from_params(params);
    let metadata = SliceMetadata::parse(slice_data).map_err(to_text)?;
    let stripe_size = metadata.stripe_size();
    let chunk_size = coder.track_chunk_size(stripe_size, metadata.blob_len());
    let plan = RepairPlan {
        lost: SliceIndex::new(0),
        chunk_size: chunk_size as u64,
        sub_chunk_size: (chunk_size / coder.alpha().max(1)) as u64,
        stripes: Vec::new(),
    };
    let slice = SliceIndex::new(0);
    let request = HelperRequest { slice, stripes: sub_chunks(stripes) };
    extract_sub_chunks(slice_data, &plan, &request).map_err(to_text)
}

fn sub_chunks(stripes: &[StripeSubChunkRequest]) -> Vec<StripeSubChunks> {
    let mut wanted = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        let sub_chunks = stripe.sub_chunks.clone();
        let stripe = stripe.stripe;
        wanted.push(StripeSubChunks { stripe, sub_chunks });
    }
    wanted
}

fn to_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::{RSParams, TAPENET_CLAY, TAPENET_PROFILE};
    use tape_core::spooler::VirtualGroup;
    use tape_core::types::SpoolIndex;

    use super::*;
    use crate::features::challenge::fixture::{coded, payload, Coded};

    const LOST: usize = 3;

    /// Every position of a tapenet group but the lost one
    fn others() -> Vec<usize> {
        let mut others = Vec::new();
        for position in 0..usize::from(TAPENET_PROFILE.n()) {
            if position != LOST {
                others.push(position);
            }
        }
        others
    }

    fn track(seed: u8) -> Coded {
        let data = payload(seed, 0, 50_000);
        coded(TAPENET_PROFILE, seed, 0, VirtualGroup(0), &data)
    }

    // a plan from every other position asks d helpers on a one-stripe track
    #[test]
    fn plans_helpers() {
        let track = track(1);
        let slicer = clay_slicer(TAPENET_PROFILE).expect("clay");
        let planner = RepairPlanner::new(slicer, LOST, &track.blob);
        let helpers = usize::from(TAPENET_CLAY.d());

        let needed = planner.plan(&others()).expect("a plan");

        assert_eq!(planner.helpers(), helpers);
        assert_eq!(needed.len(), helpers);
        assert!(!needed.contains(&LOST));
    }

    // what each helper cuts from its slice rebuilds the slice we lack
    #[test]
    fn repairs_slice() {
        let track = track(2);
        let slicer = clay_slicer(TAPENET_PROFILE).expect("clay");
        let shared = Arc::clone(&slicer);
        let planner = RepairPlanner::new(shared, LOST, &track.blob);
        let plan = planner.full_plan(&others()).expect("a plan");
        let mut held = HelperData::new(&plan);

        for request in plan.requests(&HelperData::new(&plan)) {
            let wire = wire_stripes(&request.stripes);
            let helper = &track.slices[*request.slice];
            let cut = extract_repair_data(TAPENET_PROFILE, &wire, helper);
            held.insert(&request, cut.expect("cut")).expect("insert");
        }
        let size = track.blob.size.as_usize();
        let stripe_size = track.blob.stripe_size.as_usize();
        let metadata = SliceMetadata::new(size, stripe_size).to_bytes();
        let rebuilt = slicer.repair(&plan, &held, &metadata).expect("repair");

        assert_eq!(rebuilt, track.slices[LOST]);
        let lost = SpoolIndex(LOST as u64);
        assert!(track.blob.verify_slice(lost, &rebuilt));
    }

    // a helper slice on a network with no Clay code has nothing to cut
    #[test]
    fn needs_clay() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));

        assert!(clay_slicer(profile).is_none());
        assert!(extract_repair_data(profile, &[], &[0; 64]).is_err());
    }
}
