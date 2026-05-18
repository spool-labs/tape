use std::collections::HashMap;

use rpc::Rpc;
use tape_core::erasure::group_start;
use tape_core::prelude::{GroupIndex, SpoolIndex, TrackData};
use tape_crypto::prelude::{Address, Hash};
use tape_crypto::hash::hash;
use tape_protocol::api::{ApiError, GetTrackDataReq};
use tape_protocol::Api;

use crate::codec::decoder::BlobDecoder;
use crate::codec::encoder::BlobEncoder;
use crate::error::{ClientError, TapedriveError};
use crate::metrics::{Operation, Phase};
use crate::tapedrive::Tapedrive;
use crate::track::bootstrap_network_state;
use crate::transfer::downloader::ParallelDownloader;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Read a track's data by address. No key needed, reads are public.
    pub async fn read(&self, track: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.read_as(track, Operation::ReadTrack).await
    }

    pub(crate) async fn read_as(
        &self,
        track: &Address,
        operation: Operation,
    ) -> Result<Vec<u8>, TapedriveError> {
        read_track(self, track, operation).await
    }

    /// Verify that `data` matches the on-chain commitment for a track.
    pub async fn verify(&self, track: &Address, data: &[u8]) -> Result<bool, TapedriveError> {
        let timer = self
            .timer(Operation::Verify, Phase::Total)
            .bytes(data.len() as u64);
        let result = verify_track_data(self, track, data).await;
        timer.finish_result(&result);
        result
    }
}

pub fn localize_slices(
    group: GroupIndex,
    slices: Vec<(SpoolIndex, Vec<u8>)>,
) -> Vec<(SpoolIndex, Vec<u8>)> {
    let base = group_start(group);
    slices
        .into_iter()
        .map(|(global_idx, data)| (global_idx - base, data))
        .collect()
}

pub async fn read_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
    operation: Operation,
) -> Result<Vec<u8>, TapedriveError> {
    let total = client.timer(operation, Phase::Total);
    let result = async {
        bootstrap_network_state(client, Some(operation)).await?;

        let metadata = client.timer(operation, Phase::TrackMetadata);
        let track_info = client.get_track(track).await;
        metadata.finish_result(&track_info);
        let track_info = track_info?;

        let data = client.timer(operation, Phase::TrackData);
        let track_data = fetch_track_data(client, *track, track_info.group, operation).await;
        data.finish_result(&track_data);
        let track_data = track_data?;

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

        let group = track_info.group;
        let k = blob.profile.k() as usize;

        let locate = client.timer(operation, Phase::Locate);
        let state = bootstrap_network_state(client, Some(operation)).await;
        locate.finish_result(&state);

        let state = state?;
        let slice_to_node: HashMap<SpoolIndex, Address> =
            state.group_peers(group).into_iter().collect();

        let downloader = ParallelDownloader::new(*track, slice_to_node, k);
        let download = client.timer(operation, Phase::Download);

        let slices = downloader
            .download_enough_slices(client.api.as_ref())
            .await
            .map_err(ClientError::Download);

        let download = match &slices {
            Ok(slices) => download
                .bytes(slices.iter().map(|(_, data)| data.len() as u64).sum())
                .chunks(slices.len() as u64),
            Err(_) => download,
        };
        download.finish_result(&slices);
        let slices = slices?;

        let decode = client.timer(operation, Phase::Decode);
        let mut decoder = BlobDecoder::with_profile(blob.profile);
        let data = decoder
            .decode(localize_slices(group, slices))
            .map_err(|e| TapedriveError::Download(ClientError::Decoding(e.to_string())));
        let decode = match &data {
            Ok(data) => decode.bytes(data.len() as u64),
            Err(_) => decode,
        };
        decode.finish_result(&data);
        data
    }
    .await;

    let total = match &result {
        Ok(data) => total.bytes(data.len() as u64),
        Err(_) => total,
    };
    total.finish_result(&result);
    result
}

pub async fn verify_track_data<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Address,
    data: &[u8],
) -> Result<bool, TapedriveError> {
    bootstrap_network_state(client, Some(Operation::Verify)).await?;

    let metadata = client.timer(Operation::Verify, Phase::TrackMetadata);
    let track_info = client.get_track(track).await;
    metadata.finish_result(&track_info);
    let track_info = track_info?;

    if track_info.is_raw() {
        return Ok(hash(data) == track_info.value_hash);
    }

    let data_timer = client.timer(Operation::Verify, Phase::TrackData);
    let track_data =
        fetch_track_data(client, *track, track_info.group, Operation::Verify).await;
    data_timer.finish_result(&track_data);
    let TrackData::Blob(blob) = track_data? else {
        return Err(TapedriveError::InvalidArgument(
            "expected blob track data".into(),
        ));
    };
    if blob.get_hash() != track_info.value_hash {
        return Err(TapedriveError::CommitmentMismatch);
    }

    let encode = client
        .timer(Operation::Verify, Phase::Encode)
        .bytes(data.len() as u64);
    let mut encoder = BlobEncoder::with_profile(blob.profile);
    let result = encoder
        .encode_with_root(data.to_vec())
        .map_err(|e| TapedriveError::Encoding(e.to_string()));
    encode.finish_result(&result);
    let (_, root) = result?;

    let computed: Hash = root.into();
    Ok(computed == blob.commitment)
}

async fn fetch_track_data<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: Address,
    group: GroupIndex,
    operation: Operation,
) -> Result<TrackData, TapedriveError> {
    let state = bootstrap_network_state(client, Some(operation)).await?;

    let mut peers = Vec::new();
    let mut saw_not_found = false;
    let mut last_error = None;
    for (_, node_id) in state.group_peers(group) {
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
