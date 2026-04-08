use std::collections::HashMap;

use rpc::Rpc;
use tape_core::erasure::group_start;
use tape_core::prelude::{NodeId, SpoolGroup, SpoolIndex, TrackData};
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::hash::hash;
use tape_protocol::api::{ApiError, GetTrackDataReq};
use tape_protocol::Api;

use crate::codec::decoder::BlobDecoder;
use crate::codec::encoder::BlobEncoder;
use crate::error::{ClientError, TapedriveError};
use crate::tapedrive::Tapedrive;
use crate::track::bootstrap_network_state;
use crate::transfer::downloader::ParallelDownloader;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Read a track's data by address. No key needed — reads are public.
    pub async fn read(&self, track: &Address) -> Result<Vec<u8>, TapedriveError> {
        read_track(self, track).await
    }

    /// Verify that `data` matches the on-chain commitment for a track.
    pub async fn verify(&self, track: &Address, data: &[u8]) -> Result<bool, TapedriveError> {
        verify_track_data(self, track, data).await
    }
}

pub fn localize_slices(
    spool_group: SpoolGroup,
    slices: Vec<(SpoolIndex, Vec<u8>)>,
) -> Vec<(SpoolIndex, Vec<u8>)> {
    let base = group_start(spool_group);
    slices
        .into_iter()
        .map(|(global_idx, data)| ((global_idx - base) as SpoolIndex, data))
        .collect()
}

pub async fn read_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
) -> Result<Vec<u8>, TapedriveError> {
    let track_info = client.get_track(track).await?;
    let track_data = fetch_track_data(client, *track, track_info.spool_group).await?;

    if track_info.is_raw() {
        let TrackData::Raw(bytes) = track_data else {
            return Err(TapedriveError::InvalidArgument(
                "expected raw track data".into(),
            ));
        };
        if hash(&bytes) != track_info.value_hash {
            return Err(TapedriveError::CommitmentMismatch);
        }
        return Ok(bytes);
    }

    let TrackData::Blob(blob) = track_data else {
        return Err(TapedriveError::InvalidArgument(
            "expected blob track data".into(),
        ));
    };
    if blob.get_hash() != track_info.value_hash {
        return Err(TapedriveError::CommitmentMismatch);
    }

    let spool_group = track_info.spool_group;
    let k = blob.profile.k() as usize;

    let state = bootstrap_network_state(client).await?;
    let slice_to_node: HashMap<SpoolIndex, NodeId> =
        state.group_peers(spool_group).into_iter().collect();

    let downloader = ParallelDownloader::new(
        *track,
        slice_to_node,
        k,
    );
    let slices = downloader
        .download_enough_slices(client.api.as_ref())
        .await
        .map_err(ClientError::Download)?;

    let mut decoder = BlobDecoder::with_profile(blob.profile);
    let data = decoder
        .decode(localize_slices(spool_group, slices))
        .map_err(|e| TapedriveError::Download(ClientError::Decoding(e.to_string())))?;

    Ok(data)
}

pub async fn verify_track_data<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
    data: &[u8],
) -> Result<bool, TapedriveError> {
    let track_info = client.get_track(track).await?;

    if track_info.is_raw() {
        return Ok(hash(data) == track_info.value_hash);
    }

    let TrackData::Blob(blob) = fetch_track_data(client, *track, track_info.spool_group).await? else {
        return Err(TapedriveError::InvalidArgument(
            "expected blob track data".into(),
        ));
    };
    if blob.get_hash() != track_info.value_hash {
        return Err(TapedriveError::CommitmentMismatch);
    }

    let mut encoder = BlobEncoder::with_profile(blob.profile);
    let (_, root) = encoder
        .encode_with_root(data.to_vec())
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    let computed: Hash = root.into();
    Ok(computed == blob.commitment)
}

async fn fetch_track_data<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: Address,
    spool_group: SpoolGroup,
) -> Result<TrackData, TapedriveError> {
    let state = bootstrap_network_state(client).await?;
    let mut peers = Vec::new();
    let mut saw_not_found = false;
    let mut last_error = None;
    for (_, node_id) in state.group_peers(spool_group) {
        if peers.contains(&node_id) {
            continue;
        }
        peers.push(node_id);

        let req = GetTrackDataReq { track };
        match client.api.get_track_data(node_id, &req).await {
            Ok(res) => return Ok(res.data),
            Err(ApiError::NotFound) => saw_not_found = true,
            Err(ApiError::NotResponsible) => {}
            Err(error) => last_error = Some(error),
        }
    }

    if let Some(error) = last_error {
        Err(TapedriveError::Peer(error))
    } else if saw_not_found {
        Err(TapedriveError::NotFound)
    } else {
        Err(TapedriveError::Peer(ApiError::Other(
            "no responsible peer returned track data".into(),
        )))
    }
}
