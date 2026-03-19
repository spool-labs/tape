use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_api::state::Track;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Fetch a track's on-chain state.
    pub async fn get_track(&self, track: &Pubkey) -> Result<Track, TapedriveError> {
        get_track(self, track).await
    }

    /// List all tracks on a tape.
    pub async fn list_tracks(&self, tape: &Pubkey) -> Result<Vec<(Pubkey, Track)>, TapedriveError> {
        self.rpc()
            .get_tracks_by_tape(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }
}

pub async fn retry_fetch_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    address: &Pubkey,
) -> Result<Track, TapedriveError> {
    tape_retry::retry(
        tape_retry::RetryConfig::ten(),
        None,
        || async { client.rpc().get_track_by_address(address).await },
    )
    .await
    .map_err(TapedriveError::Rpc)
}

pub async fn get_track<Blockchain: Rpc, Cluster: Api>(
    client: &Tapedrive<Blockchain, Cluster>,
    track: &Pubkey,
) -> Result<Track, TapedriveError> {
    client
        .rpc()
        .get_track_by_address(track)
        .await
        .map_err(TapedriveError::Rpc)
}

