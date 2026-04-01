use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::{NodeId, TrackNumber};
use tape_crypto::Hash;
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
    pub async fn get_track(&self, track: &Pubkey) -> Result<CompressedTrack, TapedriveError> {
        query_track(self, track).await
    }

    /// Fetch a track by its version number under a tape.
    pub async fn get_track_by_number(
        &self,
        tape: &Pubkey,
        track_number: TrackNumber,
    ) -> Result<CompressedTrack, TapedriveError> {
        query_track_by_number(self, tape, track_number).await
    }

    /// Find a track by logical key under a tape.
    pub async fn find_track(
        &self,
        tape: &Pubkey,
        key: Hash,
        version: FindTrackVersion,
    ) -> Result<CompressedTrack, TapedriveError> {
        query_find_track(self, tape, key, version).await
    }

    /// List tracks on a tape, ordered by track number, with pagination.
    pub async fn list_tracks_by_tape(
        &self,
        tape: &Pubkey,
        cursor: Option<TrackNumber>,
        limit: u32,
    ) -> Result<(Vec<CompressedTrack>, Option<TrackNumber>), TapedriveError> {
        query_tracks_by_tape(self, tape, cursor, limit).await
    }
}

pub(crate) async fn queryable_peers<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
) -> Result<Vec<NodeId>, TapedriveError> {
    let state = bootstrap_network_state(client).await?;
    let mut peers = Vec::with_capacity(state.committee.len());
    for member in &state.committee {
        if !peers.contains(&member.id) {
            peers.push(member.id);
        }
    }

    if peers.is_empty() {
        return Err(TapedriveError::Peer(ApiError::Other(
            "no committee peers available".into(),
        )));
    }

    Ok(peers)
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
    track: &Pubkey,
) -> Result<CompressedTrack, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;
    let track = (*track).into();

    for node in peers {
        let req = GetTrackReq { track };
        match client.api.get_track(node, &req).await {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_track_by_number<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Pubkey,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;
    let tape = (*tape).into();

    for node in peers {
        let req = GetTrackByNumberReq { tape, track_number };
        match client.api.get_track_by_number(node, &req).await {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_find_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Pubkey,
    key: Hash,
    version: FindTrackVersion,
) -> Result<CompressedTrack, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;
    let tape = (*tape).into();

    for node in peers {
        let req = FindTrackReq {
            tape,
            key,
            version: version.clone(),
        };
        match client.api.find_track(node, &req).await {
            Ok(res) => return Ok(res.track),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn query_tracks_by_tape<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Pubkey,
    cursor: Option<TrackNumber>,
    limit: u32,
) -> Result<(Vec<CompressedTrack>, Option<TrackNumber>), TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut last_error = None;
    let tape = (*tape).into();

    for node in peers {
        let req = ListTracksByTapeReq {
            tape,
            cursor,
            limit,
        };
        match client.api.list_tracks_by_tape(node, &req).await {
            Ok(res) => return Ok((res.tracks, res.next_cursor)),
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, false))
}

pub async fn query_track_proof<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Pubkey,
) -> Result<CompressedTrackProof, TapedriveError> {
    let peers = queryable_peers(client).await?;
    let mut saw_not_found = false;
    let mut last_error = None;
    let track = (*track).into();

    for node in peers {
        let req = GetTrackProofReq { track };
        match client.api.get_track_proof(node, &req).await {
            Ok(res) => {
                let tape_address: Pubkey = res.proof.state.tape.into();
                let tape = client
                    .rpc()
                    .get_tape_by_address(&tape_address)
                    .await
                    .map_err(TapedriveError::Rpc)?;
                if tape.tracks.verify(&res.proof).is_ok() {
                    return Ok(res.proof);
                }
                last_error = Some(ApiError::Other("stale track proof".into()));
            }
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(error) => last_error = Some(error),
        }
    }

    Err(finish_peer_query(last_error, saw_not_found))
}

pub async fn retry_fetch_track_by_number<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    tape: &Pubkey,
    track_number: TrackNumber,
) -> Result<CompressedTrack, TapedriveError> {
    tape_retry::retry(
        tape_retry::RetryConfig::ten(),
        None,
        || async { query_track_by_number(client, tape, track_number).await },
    )
    .await
}
