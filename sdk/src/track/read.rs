use std::collections::HashMap;

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_core::erasure::group_start;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::Api;

use crate::codec::decoder::BlobDecoder;
use crate::codec::encoder::BlobEncoder;
use crate::error::{ClientError, TapedriveError};
use crate::tapedrive::Tapedrive;
use crate::track::bootstrap_network_state;
use tape_crypto::Hash;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Read a track's data by address. No key needed — reads are public.
    pub async fn read(&self, track: &Pubkey) -> Result<Vec<u8>, TapedriveError> {
        read_track(self, track).await
    }

    /// Verify that `data` matches the on-chain commitment for a track.
    pub async fn verify(&self, track: &Pubkey, data: &[u8]) -> Result<bool, TapedriveError> {
        verify_track_data(self, track, data).await
    }
}

pub fn localize_slices(
    spool_group: tape_core::spooler::SpoolGroup,
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
    track: &Pubkey,
) -> Result<Vec<u8>, TapedriveError> {
    let onchain = client.rpc().get_track_by_address(track).await?;
    let spool_group = onchain.data.spool_group();
    let k = onchain.data.profile.k() as usize;

    let state = bootstrap_network_state(client).await?;
    let slice_to_node: HashMap<SpoolIndex, NodeId> =
        state.group_peers(spool_group).into_iter().collect();

    let downloader = crate::transfer::downloader::ParallelDownloader::new(
        *track,
        slice_to_node,
        k,
    );
    let slices = downloader
        .download_enough_slices(client.api.as_ref())
        .await
        .map_err(ClientError::Download)?;

    let mut decoder = BlobDecoder::with_profile(onchain.data.profile);
    let data = decoder
        .decode(localize_slices(spool_group, slices))
        .map_err(|e| TapedriveError::Download(ClientError::Decoding(e.to_string())))?;

    Ok(data)
}

pub async fn verify_track_data<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Pubkey,
    data: &[u8],
) -> Result<bool, TapedriveError> {
    let onchain = client.rpc().get_track_by_address(track).await?;

    let mut encoder = BlobEncoder::with_profile(onchain.data.profile);
    let (_, root) = encoder
        .encode_with_root(data.to_vec())
        .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

    let computed: Hash = root.into();
    Ok(computed == onchain.data.commitment_hash)
}

