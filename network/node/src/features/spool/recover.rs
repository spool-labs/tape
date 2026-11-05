//! Recover is the method we use when repair has failed. It rebuilds the whole
//! track from any 7 of its slices and keeps our slice, so it works as long as
//! 7 slices of the track survive anywhere in the spool group.
//!
//! ```text
//! ask 7 peers at different positions for their slices
//! keep each slice that matches its hash in the encoding
//! for each track that has 7 good slices:
//!   rebuild the track, and cut our slice out of it
//!   keep our slice if it matches its hash
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tape_core::encoding::ErasureCode;
use tape_core::erasure::leaf_position;
use tape_core::spooler::GroupIndex;
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::api::{ApiError, SyncSlicesReq, SyncSlicesRes};
use tape_protocol::Api;
use tape_recover::attempt::AttemptFailure;
use tape_recover::fetch::{FetchReq, FetchRes, FetchTrack};
use tape_recover::history::PositionPeer;
use tape_recover::peer::PeerOutcome;
use tape_rpc::Rpc;
use tape_slicer::{ErasureCoder, Slicer};
use tape_store_api::Store;
use tokio::task::spawn_blocking;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::error::SpoolError;
use crate::features::spool::fetch::{
    get_call_failure, get_call_outcome, is_slice_of, send_with_time_limit,
    write_slice_once,
};

/// Run one recovery fetch across its peers
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: FetchReq,
    limit: Duration,
) -> FetchRes {
    let requests = build_recover_requests(&req);
    let answers = send_recover_requests(&context, &req, &requests, limit);
    let answers = answers.await;
    let mut res = FetchRes::new(req);
    let mut slices = check_whole_slices(&mut res, answers);

    for index in 0..res.req.tracks.len() {
        let track = &res.req.tracks[index];
        let address = track.missing_track.track;
        let written = write_recovered_slice(&context, track, &mut slices);
        let outcome = written.await;
        res.add_track_result(address, outcome);
    }
    add_recover_metrics(&context, &slices, &res);
    res
}

/// Ask every position's peer for its whole slices, each at its own live
/// spool
fn build_recover_requests(req: &FetchReq) -> Vec<SyncSlicesReq> {
    let group = GroupIndex::containing(req.spool);
    let mut requests = Vec::with_capacity(req.peers.len());
    for peer in &req.peers {
        requests.push(SyncSlicesReq {
            spool_index: group.spool_at(peer.position),
            live_groups: req.live_groups,
            tracks: req.get_addresses(),
            max_bytes: req.bytes,
        });
    }
    requests
}

/// Send each position's peer its request at once
async fn send_recover_requests<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    req: &FetchReq,
    requests: &[SyncSlicesReq],
    limit: Duration,
) -> Vec<(Result<SyncSlicesRes, ApiError>, Duration)> {
    let mut calls = Vec::with_capacity(requests.len());
    for (peer, request) in req.peers.iter().zip(requests) {
        let call = context.api.sync_slices(peer.peer, request);
        calls.push(send_with_time_limit(limit, call));
    }
    join_all(calls).await
}

/// Keep each peer's slices that match its position's leaves, and record how
/// each peer did
fn check_whole_slices(
    res: &mut FetchRes,
    answers: Vec<(Result<SyncSlicesRes, ApiError>, Duration)>,
) -> WholeSlices {
    let mut slices = WholeSlices::default();
    for (index, answer) in answers.into_iter().enumerate() {
        let peer = res.req.peers[index];
        let (response, elapsed) = answer;
        let req = &res.req;
        let outcome = match response {
            Ok(response) => slices.add_answer(req, peer, response, elapsed),
            Err(error) => slices.add_failure(&error, elapsed),
        };
        res.add_peer_result(peer.peer, outcome);
    }
    slices
}

/// Rebuild one track from the slices that passed, check our slice and keep
/// it
async fn write_recovered_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    track: &FetchTrack,
    slices: &mut WholeSlices,
) -> Result<(), AttemptFailure> {
    let profile = context.params().profile;
    let address = track.missing_track.track;
    let found = slices.by_track.remove(&address).unwrap_or_default();
    if found.len() < usize::from(profile.k()) {
        return Err(slices.get_short_failure(address));
    }

    let ours = leaf_position(track.missing_track.spool);
    let position = ours.as_usize();
    let rebuild = move || rebuild_slice(profile, found, position);
    let rebuilt = match spawn_blocking(rebuild).await {
        Ok(Ok(slice)) => slice,
        Ok(Err(error)) => return log_wrong_data(address, error.to_string()),
        Err(error) => return log_wrong_data(address, error.to_string()),
    };
    if !is_slice_of(track, ours, &rebuilt) {
        let error = "rebuilt slice fails its leaf".to_string();
        return log_wrong_data(address, error);
    }

    let spool = track.missing_track.spool;
    let store = &context.store;
    if let Err(error) = write_slice_once(store, spool, address, &rebuilt) {
        return log_wrong_data(address, error.to_string());
    }
    context.metrics.add_recover_persisted(rebuilt.len() as u64);
    Ok(())
}

/// Decode the track from k slices and encode it again for our slice
fn rebuild_slice(
    profile: ErasureCode,
    slices: Vec<(usize, Vec<u8>)>,
    position: usize,
) -> Result<Vec<u8>, SpoolError> {
    let slicer = Slicer::try_from_profile(profile)?;
    let mut chunks = Vec::with_capacity(slices.len());
    for (from, bytes) in &slices {
        chunks.push((*from, bytes.as_slice()));
    }
    let data = slicer.decode(&chunks)?;
    let mut encoded = slicer.encode(&data)?;
    if position >= encoded.len() {
        let error = "position past the encoding".to_string();
        return Err(SpoolError::Slicer(error));
    }
    Ok(encoded.swap_remove(position))
}

fn log_wrong_data(
    track: Address,
    error: String,
) -> Result<(), AttemptFailure> {
    warn!(%track, %error, "spool: recovery failed");
    Err(AttemptFailure::WrongData)
}

fn add_recover_metrics<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    slices: &WholeSlices,
    res: &FetchRes,
) {
    let metrics = &context.metrics;
    metrics.add_recover_fetched(slices.bytes.as_u64());
    metrics.add_recover_rejected(slices.wrong.len() as u64);
    for (_, outcome) in &res.tracks {
        match outcome {
            Ok(()) => metrics.inc_recover_recovered(),
            Err(_) => metrics.inc_recover_put_back(),
        }
    }
}

/// What the peers sent: each track's slices that passed, by position, the
/// tracks a peer sent a wrong slice of, and the last peer failure
#[derive(Debug, Default)]
struct WholeSlices {
    by_track: HashMap<Address, Vec<(usize, Vec<u8>)>>,
    wrong: Vec<Address>,
    failure: Option<AttemptFailure>,
    bytes: StorageUnits,
}

impl WholeSlices {
    /// Keep one peer's slices that match its position's leaves, and say how
    /// the peer did
    fn add_answer(
        &mut self,
        req: &FetchReq,
        peer: PositionPeer,
        response: SyncSlicesRes,
        elapsed: Duration,
    ) -> PeerOutcome {
        let position = SpoolIndex(peer.position as u64);
        let mut received = StorageUnits(0);
        let mut is_wrong = false;
        for entry in response.entries {
            let address = Address::new(entry.track_address);
            let Some(track) = req.get_track(address) else {
                continue;
            };
            let size = StorageUnits(entry.slice_data.len() as u64);
            received = received.saturating_add(size);
            if !is_slice_of(track, position, &entry.slice_data) {
                self.wrong.push(address);
                is_wrong = true;
                continue;
            }
            let found = self.by_track.entry(address).or_default();
            found.push((peer.position, entry.slice_data));
        }
        self.bytes = self.bytes.saturating_add(received);
        if is_wrong {
            return PeerOutcome::WrongData;
        }
        PeerOutcome::Worked { bytes: received, elapsed }
    }

    /// Remember a peer's failure, and say how the peer did
    fn add_failure(
        &mut self,
        error: &ApiError,
        elapsed: Duration,
    ) -> PeerOutcome {
        let failure = get_call_failure(error);
        if failure != AttemptFailure::NotHeld {
            self.failure = Some(failure);
        }
        get_call_outcome(error, elapsed)
    }

    /// Too few slices: wrong data when a peer sent a bad one, a peer's
    /// failure when one failed, and doesn't have it otherwise
    fn get_short_failure(&self, track: Address) -> AttemptFailure {
        if self.wrong.contains(&track) {
            return AttemptFailure::WrongData;
        }
        self.failure.unwrap_or(AttemptFailure::NotHeld)
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
            slices.push((position, track.slices[position].clone()));
        }

        let rebuilt = rebuild_slice(TAPENET_PROFILE, slices, 3);
        let rebuilt = rebuilt.expect("rebuild");

        assert_eq!(rebuilt, track.slices[3]);
        assert!(track.blob.verify_slice(SpoolIndex(3), &rebuilt));
    }
}
