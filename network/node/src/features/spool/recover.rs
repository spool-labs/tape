//! Recovery: k whole slices from k positions, the track rebuilt from them,
//! and our slice cut out of it
//!
//! Build the requests, send them, check what came back, write it, report.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tape_core::encoding::ErasureCode;
use tape_core::erasure::leaf_position;
use tape_core::spooler::GroupIndex;
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::api::SyncSlicesRes;
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_slicer::{ErasureCoder, Slicer};
use tape_store::types::{AttemptResult, EntryKey};
use tape_store_api::Store;
use tokio::task::spawn_blocking;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::attempt::Helper;
use crate::features::spool::error::SpoolError;
use crate::features::spool::fetch::{
    failed_track, send, Answer, CallFailure, Checked, Fetch, FetchReport,
    FetchTrack, TrackOutcome,
};
use crate::features::spool::peers::PeerOutcome;
use crate::features::spool::transfer::{
    build_request, check, is_slice_of, write_slice,
};

/// One position's slice of a track
#[derive(Clone, Debug)]
struct PositionSlice {
    position: usize,
    bytes: Vec<u8>,
}

/// What one position's peer sent, checked against its leaves
#[derive(Debug)]
struct PositionAnswer {
    helper: Helper,
    checked: Result<Checked, CallFailure>,
}

/// The slices that passed their check, by track
type SlicesByTrack = HashMap<Address, Vec<PositionSlice>>;

/// Run one recovery fetch across its peers
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    fetch: Fetch,
    limit: Duration,
) -> FetchReport {
    let answers = ask_all(&ctx, &fetch, limit).await;
    let mut report = FetchReport::new(fetch);
    let mut slices = SlicesByTrack::new();
    let mut checked = Vec::with_capacity(answers.len());
    for (index, answer) in answers.into_iter().enumerate() {
        let helper = report.fetch.peers[index];
        let mut answer = check_answer(&mut report, helper, answer);
        take_slices(&mut answer, &mut slices);
        checked.push(answer);
    }

    let profile = ctx.params().profile;
    let ours = leaf_position(report.fetch.spool);
    for index in 0..report.fetch.tracks.len() {
        let track = &report.fetch.tracks[index];
        let key = track.key;
        let found = slices.remove(&key.track).unwrap_or_default();
        let answers = &checked;
        let rebuilt = recover_track(&ctx, profile, ours, track, found, answers);
        let outcome = rebuilt.await;
        count(&ctx, outcome);
        report.track(key, outcome);
    }
    count_answers(&ctx, &checked);
    report
}

/// Ask every position's peer at once, each at its own live spool
async fn ask_all<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    fetch: &Fetch,
    limit: Duration,
) -> Vec<Answer<SyncSlicesRes>> {
    let group = GroupIndex::containing(fetch.spool);
    let mut requests = Vec::with_capacity(fetch.peers.len());
    for helper in &fetch.peers {
        requests.push(build_request(fetch, group.spool_at(helper.position)));
    }

    let mut calls = Vec::with_capacity(requests.len());
    for (helper, request) in fetch.peers.iter().zip(&requests) {
        let call = ctx.api.sync_slices(helper.peer, request);
        calls.push(send(limit, call));
    }
    join_all(calls).await
}

/// Check one peer's slices against its position's leaves, and record how
/// the peer did
fn check_answer(
    report: &mut FetchReport,
    helper: Helper,
    answer: Answer<SyncSlicesRes>,
) -> PositionAnswer {
    let position = SpoolIndex(helper.position as u64);
    let checked = match answer.response {
        Ok(response) => Ok(check(&report.fetch, position, response)),
        Err(failure) => Err(failure),
    };
    let outcome = match &checked {
        Ok(checked) if !checked.wrong.is_empty() => PeerOutcome::WrongData,
        Ok(checked) => worked(checked.received, answer.elapsed),
        Err(CallFailure::NotHeld) => worked(StorageUnits(0), answer.elapsed),
        Err(CallFailure::Peer(outcome)) => *outcome,
    };
    report.peer(helper.peer, outcome);
    PositionAnswer { helper, checked }
}

fn worked(bytes: StorageUnits, elapsed: Duration) -> PeerOutcome {
    PeerOutcome::Worked { bytes, elapsed }
}

/// Move a peer's good slices into the slices by track
fn take_slices(answer: &mut PositionAnswer, slices: &mut SlicesByTrack) {
    let Ok(checked) = &mut answer.checked else {
        return;
    };
    let position = answer.helper.position;
    for fetched in std::mem::take(&mut checked.good) {
        let slice = PositionSlice { position, bytes: fetched.bytes };
        slices.entry(fetched.key.track).or_default().push(slice);
    }
}

/// Rebuild one track from the slices that passed, and keep our slice
async fn recover_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    profile: ErasureCode,
    ours: SpoolIndex,
    track: &FetchTrack,
    slices: Vec<PositionSlice>,
    answers: &[PositionAnswer],
) -> TrackOutcome {
    if slices.len() < usize::from(profile.k()) {
        return short_outcome(track.key, answers);
    }

    let position = ours.as_usize();
    let rebuild = move || rebuild_slice(profile, &slices, position);
    let rebuilt = match spawn_blocking(rebuild).await {
        Ok(Ok(slice)) => slice,
        Ok(Err(error)) => return wrong(track.key, error.to_string()),
        Err(error) => return wrong(track.key, error.to_string()),
    };
    if !is_slice_of(track, ours, &rebuilt) {
        return wrong(track.key, "rebuilt slice fails its leaf".to_string());
    }

    let size = rebuilt.len() as u64;
    match write_slice(ctx, track.key.spool, track.key.track, rebuilt) {
        Ok(()) => {
            ctx.metrics.add_recover_persisted(size);
            TrackOutcome::Got
        }
        Err(error) => wrong(track.key, error.to_string()),
    }
}

fn wrong(key: EntryKey, error: String) -> TrackOutcome {
    warn!(track = %key.track, %error, "spool: recovery failed");
    TrackOutcome::Failed(AttemptResult::WrongData)
}

/// Too few slices: wrong data when a peer sent a bad one, a peer's failure
/// when one failed, and doesn't have it otherwise
fn short_outcome(key: EntryKey, answers: &[PositionAnswer]) -> TrackOutcome {
    let mut failure = None;
    for answer in answers {
        if sent_wrong(answer, key) {
            return TrackOutcome::Failed(AttemptResult::WrongData);
        }
        if let Err(CallFailure::Peer(outcome)) = answer.checked {
            failure = Some(outcome);
        }
    }
    match failure {
        Some(outcome) => failed_track(outcome),
        None => TrackOutcome::Failed(AttemptResult::NotHeld),
    }
}

fn sent_wrong(answer: &PositionAnswer, key: EntryKey) -> bool {
    match &answer.checked {
        Ok(checked) => checked.wrong.contains(&key),
        Err(_) => false,
    }
}

/// Decode the track from k slices and encode it again for our slice
fn rebuild_slice(
    profile: ErasureCode,
    slices: &[PositionSlice],
    position: usize,
) -> Result<Vec<u8>, SpoolError> {
    let slicer = Slicer::try_from_profile(profile).map_err(slicer_error)?;
    let mut chunks = Vec::with_capacity(slices.len());
    for slice in slices {
        chunks.push((slice.position, slice.bytes.as_slice()));
    }
    let data = slicer.decode(&chunks).map_err(slicer_error)?;
    let mut encoded = slicer.encode(&data).map_err(slicer_error)?;
    if position >= encoded.len() {
        return Err(SpoolError::Slicer("position past the encoding".into()));
    }
    Ok(encoded.swap_remove(position))
}

fn slicer_error(error: impl std::fmt::Display) -> SpoolError {
    SpoolError::Slicer(error.to_string())
}

fn count<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    outcome: TrackOutcome,
) {
    match outcome {
        TrackOutcome::Got => ctx.metrics.inc_recover_recovered(),
        TrackOutcome::Failed(_) => ctx.metrics.inc_recover_put_back(),
    }
}

fn count_answers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    answers: &[PositionAnswer],
) {
    for answer in answers {
        let Ok(checked) = &answer.checked else {
            continue;
        };
        ctx.metrics.add_recover_fetched(checked.received.as_u64());
        ctx.metrics.add_recover_rejected(checked.wrong.len() as u64);
    }
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;

    use super::*;
    use crate::features::challenge::fixture::{coded, payload};

    // seven slices rebuild the track and give back the very slice we lack
    #[test]
    fn rebuilds_slice() {
        let data = payload(1, 0, 50_000);
        let track = coded(TAPENET_PROFILE, 1, 0, VirtualGroup(0), &data);
        let mut slices = Vec::new();
        for position in [1, 4, 7, 9, 12, 15, 19] {
            let bytes = track.slices[position].clone();
            slices.push(PositionSlice { position, bytes });
        }

        let rebuilt = rebuild_slice(TAPENET_PROFILE, &slices, 3);
        let rebuilt = rebuilt.expect("rebuild");

        assert_eq!(rebuilt, track.slices[3]);
        assert!(track.blob.verify_slice(SpoolIndex(3), &rebuilt));
    }
}
