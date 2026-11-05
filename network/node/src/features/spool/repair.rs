//! Repair is the method we use when none of the spool's past owners can give
//! us our slice. It rebuilds the slice from small parts of 16 other slices of
//! the same track, which reads far fewer bytes than rebuilding the whole
//! track.
//!
//! ```text
//! plan which parts each of the 16 helpers, the nodes that hold those slices,
//!   needs to send
//! ask all of the helpers for their parts at the same time
//! rebuild our slice from what they sent
//! keep the slice if it matches its hash in the encoding
//! if a helper sent wrong data, the track goes to recovery next
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tape_core::encoding::ErasureCode;
use tape_core::erasure::leaf_position;
use tape_core::track::blob::BlobEncoding;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_protocol::api::{
    ApiError, RepairReq, RepairRes, StripeSubChunkRequest,
};
use tape_protocol::Api;
use tape_recover::attempt::AttemptFailure;
use tape_recover::fetch::{FetchReq, FetchRes, FetchTrack};
use tape_recover::history::PositionPeer;
use tape_recover::peer::PeerOutcome;
use tape_rpc::Rpc;
use tape_slicer::{
    ClayCoder, HelperData, HelperRequest, RepairError, RepairPlan,
    SliceIndex, SliceMetadata, Slicer, StripeSubChunks,
};
use tape_store_api::Store;
use tokio::task::spawn_blocking;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::fetch::{
    get_call_failure, get_call_outcome, is_slice_of, send_with_time_limit,
    write_slice_once,
};

/// Run one repair fetch for its one track
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: FetchReq,
    limit: Duration,
) -> FetchRes {
    let mut res = FetchRes::new(req);
    let Some(track) = res.req.tracks.first().cloned() else {
        return res;
    };
    let profile = context.params().profile;
    let position = leaf_position(res.req.spool).as_usize();
    let encoding = track.encoding.as_ref();
    let helpers = &res.req.peers;
    let planned = RepairPlanner::try_new(profile, position, encoding, helpers);
    let Some(planner) = planned else {
        res.fail_every_track(AttemptFailure::NotHeld);
        return res;
    };

    let requests = planner.get_requests();
    let wire = build_repair_requests(&track, &requests);
    let answers = send_repair_requests(&context, &res.req, &wire, limit).await;
    let mut data = HelperData::new(&planner.plan);
    let failures = check_helper_data(&mut res, &requests, answers, &mut data);
    let outcome = match get_repair_failure(&failures) {
        Some(failure) => Err(failure),
        None => write_repaired_slice(&context, &track, planner, data).await,
    };
    add_repair_metrics(&context, &res, &failures, outcome);
    res.add_track_result(track.missing_track.track, outcome);
    res
}

/// Each helper's request on the wire, asking at its own stored spool
fn build_repair_requests(
    track: &FetchTrack,
    requests: &[HelperRequest],
) -> Vec<RepairReq> {
    let mut wire = Vec::with_capacity(requests.len());
    for request in requests {
        wire.push(RepairReq {
            track: track.missing_track.track,
            helper_spool: track.track.group.spool_at(*request.slice),
            stripes: get_wire_stripes(&request.stripes),
        });
    }
    wire
}

fn get_wire_stripes(
    stripes: &[StripeSubChunks],
) -> Vec<StripeSubChunkRequest> {
    let mut wire = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        let sub_chunks = stripe.sub_chunks.clone();
        let stripe = stripe.stripe;
        wire.push(StripeSubChunkRequest { stripe, sub_chunks });
    }
    wire
}

/// Send each helper its request at once; a request whose position has no
/// helper in the fetch gets no answer
async fn send_repair_requests<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    req: &FetchReq,
    wire: &[RepairReq],
    limit: Duration,
) -> Vec<Option<(Result<RepairRes, ApiError>, Duration)>> {
    let mut calls = Vec::with_capacity(wire.len());
    for request in wire {
        let position = leaf_position(request.helper_spool).as_usize();
        let peer = get_helper_peer(req, position);
        calls.push(send_to_helper(context, peer, request, limit));
    }
    join_all(calls).await
}

fn get_helper_peer(req: &FetchReq, position: usize) -> Option<Address> {
    for helper in &req.peers {
        if helper.position == position {
            return Some(helper.peer);
        }
    }
    None
}

async fn send_to_helper<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    peer: Option<Address>,
    request: &RepairReq,
    limit: Duration,
) -> Option<(Result<RepairRes, ApiError>, Duration)> {
    let call = context.api.repair(peer?, request);
    Some(send_with_time_limit(limit, call).await)
}

/// Keep what each helper sent and record how it did; give the failures of
/// the helpers that fell short
fn check_helper_data(
    res: &mut FetchRes,
    requests: &[HelperRequest],
    answers: Vec<Option<(Result<RepairRes, ApiError>, Duration)>>,
    data: &mut HelperData,
) -> Vec<AttemptFailure> {
    let mut failures = Vec::new();
    for (request, answer) in requests.iter().zip(answers) {
        let Some(answer) = answer else {
            continue;
        };
        let Some(peer) = get_helper_peer(&res.req, *request.slice) else {
            continue;
        };
        let (response, elapsed) = answer;
        let outcome = check_helper_answer(
            data,
            request,
            response,
            elapsed,
            &mut failures,
        );
        res.add_peer_result(peer, outcome);
    }
    failures
}

/// Keep one helper's sub-chunks, and say how the helper did: wrong data
/// when they don't fit the plan, and its failure when it fell short
fn check_helper_answer(
    data: &mut HelperData,
    request: &HelperRequest,
    response: Result<RepairRes, ApiError>,
    elapsed: Duration,
    failures: &mut Vec<AttemptFailure>,
) -> PeerOutcome {
    let sent = match response {
        Ok(sent) => sent,
        Err(error) => {
            failures.push(get_call_failure(&error));
            return get_call_outcome(&error, elapsed);
        }
    };
    let bytes = StorageUnits(sent.data.len() as u64);
    if data.insert(request, sent.data).is_err() {
        failures.push(AttemptFailure::WrongData);
        return PeerOutcome::WrongData;
    }
    PeerOutcome::Worked { bytes, elapsed }
}

/// The track's failure when a helper fell short: wrong data first, then
/// doesn't have it, then the last peer failure
fn get_repair_failure(failures: &[AttemptFailure]) -> Option<AttemptFailure> {
    if failures.contains(&AttemptFailure::WrongData) {
        return Some(AttemptFailure::WrongData);
    }
    if failures.contains(&AttemptFailure::NotHeld) {
        return Some(AttemptFailure::NotHeld);
    }
    failures.last().copied()
}

/// Rebuild our slice from what the helpers sent, check it, and keep it
async fn write_repaired_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    track: &FetchTrack,
    planner: RepairPlanner,
    data: HelperData,
) -> Result<(), AttemptFailure> {
    let address = track.missing_track.track;
    let Some(encoding) = track.encoding else {
        return log_wrong_data(address, "no encoding".to_string());
    };
    let rebuild = move || planner.rebuild(&encoding, data);
    let rebuilt = match spawn_blocking(rebuild).await {
        Ok(Ok(slice)) => slice,
        Ok(Err(error)) => return log_wrong_data(address, error.to_string()),
        Err(error) => return log_wrong_data(address, error.to_string()),
    };

    let ours = leaf_position(track.missing_track.spool);
    if !is_slice_of(track, ours, &rebuilt) {
        let error = "repaired slice fails its leaf".to_string();
        return log_wrong_data(address, error);
    }
    let spool = track.missing_track.spool;
    let store = &context.store;
    if let Err(error) = write_slice_once(store, spool, address, &rebuilt) {
        return log_wrong_data(address, error.to_string());
    }
    context.metrics.add_repair_persisted(rebuilt.len() as u64);
    Ok(())
}

fn log_wrong_data(
    track: Address,
    error: String,
) -> Result<(), AttemptFailure> {
    warn!(%track, %error, "spool: repair failed");
    Err(AttemptFailure::WrongData)
}

fn add_repair_metrics<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    res: &FetchRes,
    failures: &[AttemptFailure],
    outcome: Result<(), AttemptFailure>,
) {
    let metrics = &context.metrics;
    let mut fetched = StorageUnits(0);
    for (_, outcome) in &res.peers {
        if let PeerOutcome::Worked { bytes, .. } = outcome {
            fetched = fetched.saturating_add(*bytes);
        }
    }
    metrics.add_repair_fetched(fetched.as_u64());
    for failure in failures {
        metrics.inc_repair_missing(get_miss_cause(*failure));
    }
    match outcome {
        Ok(()) => metrics.inc_repair_repaired(),
        Err(_) => metrics.inc_repair_put_back(),
    }
}

/// The cause a helper that fell short counts under
fn get_miss_cause(failure: AttemptFailure) -> &'static str {
    match failure {
        AttemptFailure::WrongData => "wrong_data",
        AttemptFailure::NotHeld => "not_held",
        AttemptFailure::TooSlow => "deadline",
        AttemptFailure::Down => "down",
        AttemptFailure::Refused => "refused",
    }
}

/// The Clay plan for one track's slice at one position, from d helpers
struct RepairPlanner {
    slicer: Slicer<ClayCoder>,
    plan: RepairPlan,
}

impl RepairPlanner {
    /// The plan from these helpers, or nothing off a Clay code, without the
    /// track's encoding, or when the helpers give no plan for every stripe
    fn try_new(
        profile: ErasureCode,
        position: usize,
        encoding: Option<&BlobEncoding>,
        helpers: &[PositionPeer],
    ) -> Option<Self> {
        let ErasureCode::Clay(params) = profile else {
            return None;
        };
        let encoding = encoding?;
        let slicer = Slicer::with_rotation(ClayCoder::from_params(params));
        let mut available = Vec::with_capacity(helpers.len());
        for helper in helpers {
            available.push(SliceIndex::new(helper.position));
        }
        let lost = SliceIndex::new(position);
        let size = encoding.size.as_usize();
        let stripe_size = encoding.stripe_size.as_usize();
        let planned = slicer.repair_plan(lost, &available, size, stripe_size);
        let plan = planned.ok()?;
        for stripe in &plan.stripes {
            stripe.helpers.as_ref()?;
        }
        Some(Self { slicer, plan })
    }

    /// What the plan asks of each helper it needs
    fn get_requests(&self) -> Vec<HelperRequest> {
        self.plan.requests(&HelperData::new(&self.plan))
    }

    /// Rebuild the lost slice from the sub-chunks the helpers sent
    fn rebuild(
        self,
        encoding: &BlobEncoding,
        data: HelperData,
    ) -> Result<Vec<u8>, RepairError> {
        let size = encoding.size.as_usize();
        let stripe_size = encoding.stripe_size.as_usize();
        let metadata = SliceMetadata::new(size, stripe_size).to_bytes();
        self.slicer.repair(&self.plan, &data, &metadata)
    }
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::{RSParams, TAPENET_CLAY, TAPENET_PROFILE};
    use tape_core::spooler::VirtualGroup;
    use tape_core::types::SpoolIndex;

    use super::*;
    use crate::features::challenge::fixture::{coded, payload, Coded};
    use crate::features::http::handlers::track::repair::extract_repair_data;

    const LOST: usize = 3;

    fn has_position(helpers: &[PositionPeer], position: usize) -> bool {
        for helper in helpers {
            if helper.position == position {
                return true;
            }
        }
        false
    }

    /// d helpers of a tapenet group, none at the lost position
    fn helpers() -> Vec<PositionPeer> {
        let d = usize::from(TAPENET_CLAY.d());
        let mut helpers = Vec::new();
        for position in 0..usize::from(TAPENET_PROFILE.n()) {
            if position != LOST && helpers.len() < d {
                let peer = Address::new([position as u8; 32]);
                helpers.push(PositionPeer { position, peer });
            }
        }
        helpers
    }

    fn track(seed: u8) -> Coded {
        let data = payload(seed, 0, 50_000);
        coded(TAPENET_PROFILE, seed, 0, VirtualGroup(0), &data)
    }

    // a plan from d helpers asks only the helpers it was given
    #[test]
    fn plans_helpers() {
        let track = track(1);
        let helpers = helpers();
        let blob = Some(&track.blob);

        let planner = RepairPlanner::try_new(TAPENET_PROFILE, LOST, blob, &helpers);

        let requests = planner.expect("a plan").get_requests();
        assert!(!requests.is_empty());
        for request in requests {
            let position = *request.slice;
            assert_ne!(position, LOST);
            assert!(has_position(&helpers, position));
        }
    }

    // what each helper cuts from its slice rebuilds the slice we lack
    #[test]
    fn repairs_slice() {
        let track = track(2);
        let blob = Some(&track.blob);
        let planner = RepairPlanner::try_new(TAPENET_PROFILE, LOST, blob, &helpers());
        let planner = planner.expect("a plan");
        let mut data = HelperData::new(&planner.plan);

        for request in planner.get_requests() {
            let wire = get_wire_stripes(&request.stripes);
            let helper = &track.slices[*request.slice];
            let cut = extract_repair_data(TAPENET_PROFILE, &wire, helper);
            data.insert(&request, cut.expect("cut")).expect("insert");
        }
        let rebuilt = planner.rebuild(&track.blob, data).expect("repair");

        assert_eq!(rebuilt, track.slices[LOST]);
        let lost = SpoolIndex(LOST as u64);
        assert!(track.blob.verify_slice(lost, &rebuilt));
    }

    // a network with no Clay code has no repair plan
    #[test]
    fn needs_clay() {
        let track = track(3);
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let blob = Some(&track.blob);

        let planner = RepairPlanner::try_new(profile, LOST, blob, &helpers());

        assert!(planner.is_none());
    }
}
