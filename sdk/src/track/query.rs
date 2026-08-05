use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use rpc::Rpc;
use tokio::time::{Instant as TokioInstant, sleep};
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_core::types::TrackNumber;
use tape_protocol::api::{
    ApiError, FindTrackReq, FindTrackRes, FindTrackVersion, GetTrackByNumberReq,
    GetTrackByNumberRes, GetTrackProofReq, GetTrackProofRes, GetTrackReq, GetTrackRes,
    ListTracksByTapeReq, ListTracksByTapeRes,
};
use tape_protocol::Api;

use crate::bootstrap::{HEDGE_DELAY, counts_against_peer};
use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;
use crate::track::bootstrap_network_state;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {

    /// Fetch a track's current compressed state from peers.
    pub async fn get_track(&self, track: &Address) -> Result<CompressedTrack, TapedriveError> {
        query_track(self, track).await
    }

    /// Fetch a track by its version number under a tape.
    pub async fn get_track_by_number(
        &self,
        tape: &Address,
        track_number: TrackNumber,
    ) -> Result<CompressedTrack, TapedriveError> {
        query_track_by_number(self, tape, track_number).await
    }

    /// Find a track by logical key under a tape.
    pub async fn find_track(
        &self,
        tape: &Address,
        key: Hash,
        version: FindTrackVersion,
    ) -> Result<CompressedTrack, TapedriveError> {
        query_find_track(self, tape, key, version).await
    }

    /// List tracks on a tape, ordered by track number, with pagination.
    pub async fn list_tracks_by_tape(
        &self,
        tape: &Address,
        cursor: Option<TrackNumber>,
        limit: u32,
    ) -> Result<(Vec<CompressedTrack>, Option<TrackNumber>), TapedriveError> {
        query_tracks_by_tape(self, tape, cursor, limit).await
    }

    /// Fetch a verifiable proof for a track (used to delete it).
    pub async fn get_track_proof(
        &self,
        track: &Address,
    ) -> Result<CompressedTrackProof, TapedriveError> {
        query_track_proof(self, track).await
    }
}

pub(crate) async fn queryable_peers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
) -> Result<Vec<Address>, TapedriveError> {
    let state = bootstrap_network_state(client, None).await?;
    let mut peers = Vec::with_capacity(state.current.committee.len());
    for member in &state.current.committee {
        if !peers.contains(&member.node) {
            peers.push(member.node);
        }
    }

    if peers.is_empty() {
        return Err(TapedriveError::Peer(ApiError::Other(
            "no committee peers available".into(),
        )));
    }

    Ok(peers)
}

/// Race one request per peer, yielding results in completion order. The
/// caller returns on the first useful response; dropping the stream cancels
/// the rest.
pub(crate) fn race_peers<PeerFuture>(
    peers: Vec<Address>,
    call: impl Fn(Address) -> PeerFuture,
) -> FuturesUnordered<PeerFuture>
where
    PeerFuture: std::future::Future,
{
    peers.into_iter().map(call).collect()
}

/// Ask peers in a widening ladder rather than all at once.
///
/// The old behaviour sent one request per committee member, so a full
/// committee meant up to MEMBER_COUNT requests for every metadata lookup, and
/// every lookup rediscovered the dead ones. Here the best few peers are asked
/// first and the circle widens only while nobody has answered, which keeps the
/// latency of a race without its cost.
///
/// Outcomes feed reputation, so the ordering improves as the client runs.
pub(crate) async fn query_ladder<Blockchain, Cluster, PeerFuture, AcceptFuture, T>(
    client: &Tapedrive<Blockchain, Cluster>,
    call: impl Fn(Address) -> PeerFuture,
    accept: impl Fn(T) -> AcceptFuture,
) -> Result<T, TapedriveError>
where
    Blockchain: Rpc,
    Cluster: Api,
    PeerFuture: std::future::Future<Output = Result<T, ApiError>>,
    AcceptFuture: std::future::Future<Output = Result<Option<T>, TapedriveError>>,
{
    let peers = queryable_peers(client).await?;
    let ordered = client.reputation.order(&peers);
    let width = client.read_options.query_fan_out.max(1);

    let mut saw_not_found = false;
    let mut last_error = None;
    let mut launched = 0usize;
    let mut pending = FuturesUnordered::new();

    let launch = |index: usize, pending: &mut FuturesUnordered<_>| {
        let node = ordered[index];
        let started = Instant::now();
        let future = call(node);
        pending.push(async move { (node, started, future.await) });
    };

    while launched < width.min(ordered.len()) {
        launch(launched, &mut pending);
        launched += 1;
    }

    let hedge = sleep(HEDGE_DELAY);
    tokio::pin!(hedge);

    loop {
        tokio::select! {
            biased;

            // Safe: next() is cancellation-safe, an unfinished request stays in the set
            Some((node, started, result)) = pending.next() => {
                match result {
                    Ok(value) => {
                        client.reputation.record_success(node, started.elapsed());
                        match accept(value).await? {
                            Some(accepted) => return Ok(accepted),
                            None => last_error = Some(ApiError::StaleTrackProof),
                        }
                    }
                    Err(error) => {
                        // A peer that answers "no" is healthy, it simply lacks
                        // the record, and the same goes for backpressure or a
                        // spool it does not own.
                        match counts_against_peer(&error) {
                            true => client.reputation.record_failure(node),
                            false => client.reputation.record_success(node, started.elapsed()),
                        }
                        // Absence is reported as absence, not as the last
                        // transport error, so it stays out of the error slot.
                        match error {
                            ApiError::NotFound => saw_not_found = true,
                            error => last_error = Some(error),
                        }
                        // The slot is free, so take the next peer straight away.
                        // Waiting out the hedge here would spend it on a peer we
                        // already know did not answer.
                        if launched < ordered.len() {
                            launch(launched, &mut pending);
                            launched += 1;
                            hedge.as_mut().reset(TokioInstant::now() + HEDGE_DELAY);
                        }
                    }
                }
            }

            // Safe: the sleep is pinned, so a lost race resumes it
            _ = &mut hedge, if launched < ordered.len() => {
                launch(launched, &mut pending);
                launched += 1;
                hedge.as_mut().reset(TokioInstant::now() + HEDGE_DELAY);
            }

            else => break,
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

fn finish_peer_query(last_error: Option<ApiError>, saw_not_found: bool) -> TapedriveError {
    if let Some(error) = last_error {
        TapedriveError::from(error)
    } else if saw_not_found {
        TapedriveError::NotFound
    } else {
        TapedriveError::Peer(ApiError::Other(
            "no responsive peers available".into(),
        ))
    }
}

pub async fn query_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
) -> Result<CompressedTrack, TapedriveError> {
    query_ladder(
        client,
        |node| {
            let req = GetTrackReq { track: *track };
            async move { client.api.get_track(node, &req).await }
        },
        |res: GetTrackRes| async move { Ok(Some(res)) },
    )
    .await
    .map(|res| res.track)
}

pub async fn query_track_by_number<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    query_ladder(
        client,
        |node| {
            let req = GetTrackByNumberReq { tape: *tape, track_number };
            async move { client.api.get_track_by_number(node, &req).await }
        },
        |res: GetTrackByNumberRes| async move { Ok(Some(res)) },
    )
    .await
    .map(|res| res.track)
}

pub async fn query_find_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    key: Hash,
    version: FindTrackVersion,
) -> Result<CompressedTrack, TapedriveError> {
    query_ladder(
        client,
        |node| {
            let req = FindTrackReq {
                tape: *tape,
                key,
                version: version.clone(),
            };
            async move { client.api.find_track(node, &req).await }
        },
        |res: FindTrackRes| async move { Ok(Some(res)) },
    )
    .await
    .map(|res| res.track)
}

pub async fn query_tracks_by_tape<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    cursor: Option<TrackNumber>,
    limit: u32,
) -> Result<(Vec<CompressedTrack>, Option<TrackNumber>), TapedriveError> {
    query_ladder(
        client,
        |node| {
            let req = ListTracksByTapeReq {
                tape: *tape,
                cursor,
                limit,
            };
            async move { client.api.list_tracks_by_tape(node, &req).await }
        },
        |res: ListTracksByTapeRes| async move { Ok(Some(res)) },
    )
    .await
    .map(|res| (res.tracks, res.next_cursor))
}

pub async fn query_track_proof<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
) -> Result<CompressedTrackProof, TapedriveError> {
    query_ladder(
        client,
        |node| {
            let req = GetTrackProofReq { track: *track };
            async move { client.api.get_track_proof(node, &req).await }
        },
        // A proof can be honestly served and still be stale, so it is checked
        // against the tape before it counts as an answer. Rejecting one keeps
        // the ladder going rather than failing the query.
        |res: GetTrackProofRes| async move {
            let tape = client
                .rpc()
                .get_tape_by_address(&res.proof.state.tape)
                .await
                .map_err(TapedriveError::Rpc)?;
            match tape.tracks.verify(&res.proof).is_ok() {
                true => Ok(Some(res)),
                false => Ok(None),
            }
        },
    )
    .await
    .map(|res| res.proof)
}
