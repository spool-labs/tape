use futures::stream::{FuturesUnordered, StreamExt};
use rpc::Rpc;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_core::types::TrackNumber;
use tape_protocol::api::{
    ApiError, FindTrackReq, FindTrackVersion, GetTrackByNumberReq, GetTrackProofReq,
    GetTrackReq, ListTracksByTapeReq,
};
use tape_protocol::Api;

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

fn finish_peer_query(last_error: Option<ApiError>, saw_not_found: bool) -> TapedriveError {
    if let Some(error) = last_error {
        TapedriveError::Peer(error)
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
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;

    let mut requests = race_peers(peers, |node| {
        let req = GetTrackReq { track: *track };
        async move { client.api.get_track(node, &req).await }
    });
    while let Some(result) = requests.next().await {
        match result {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_track_by_number<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;

    let mut requests = race_peers(peers, |node| {
        let req = GetTrackByNumberReq { tape: *tape, track_number };
        async move { client.api.get_track_by_number(node, &req).await }
    });
    while let Some(result) = requests.next().await {
        match result {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_find_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    key: Hash,
    version: FindTrackVersion,
) -> Result<CompressedTrack, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;

    let mut requests = race_peers(peers, |node| {
        let req = FindTrackReq {
            tape: *tape,
            key,
            version: version.clone(),
        };
        async move { client.api.find_track(node, &req).await }
    });
    while let Some(result) = requests.next().await {
        match result {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_tracks_by_tape<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Address,
    cursor: Option<TrackNumber>,
    limit: u32,
) -> Result<(Vec<CompressedTrack>, Option<TrackNumber>), TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut last_error = None;

    let mut requests = race_peers(peers, |node| {
        let req = ListTracksByTapeReq {
            tape: *tape,
            cursor,
            limit,
        };
        async move { client.api.list_tracks_by_tape(node, &req).await }
    });
    while let Some(result) = requests.next().await {
        match result {
            Ok(res) => return Ok((res.tracks, res.next_cursor)),
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, false))
}

pub async fn query_track_proof<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
) -> Result<CompressedTrackProof, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;

    let mut requests = race_peers(peers, |node| {
        let req = GetTrackProofReq { track: *track };
        async move { client.api.get_track_proof(node, &req).await }
    });
    while let Some(result) = requests.next().await {
        match result {
            Ok(res) => {
                let tape_address: Address = res.proof.state.tape.into();
                let tape = client
                    .rpc()
                    .get_tape_by_address(&tape_address)
                    .await
                    .map_err(TapedriveError::Rpc)?;
                if tape.tracks.verify(&res.proof).is_ok() {
                    return Ok(res.proof);
                }
                last_error = Some(ApiError::StaleTrackProof);
            }
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}
