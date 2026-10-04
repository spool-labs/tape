//! The chain through `tape-sdk`: inline writes and deletes, reservations and extensions

use tape_peer_http::HttpApi;
use tape_rpc::RpcError;
use tape_rpc_solana::{RpcConfig, SolanaRpc};
use tape_api::program::tapedrive::TrackAddress;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::helpers::load_ed25519_keypair;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

use crate::chain::error::ChainError;
use crate::chain::keys::load_key;
use crate::chain::tape_chain::{PayerBalances, TapeChain, TapeStatus};
use crate::config::settings::ChainConfig;
use crate::publisher::writer::{TrackWriter, WrittenTrack};
use crate::records::rows::ListTape;
use crate::types::ids::TrackCount;

/// A Tapedrive client paying with the configured payer
pub struct SdkChain {
    client: Tapedrive<SolanaRpc, HttpApi>,
}

impl SdkChain {
    pub fn try_new(config: &ChainConfig) -> Result<Self, ChainError> {
        let payer = load_ed25519_keypair(&config.payer_key_file).map_err(|error| ChainError::PayerKey {
            path: config.payer_key_file.clone(),
            reason: error.to_string(),
        })?;
        let rpc = SolanaRpc::new(RpcConfig {
            endpoints: vec![config.rpc_url.clone()],
            ..Default::default()
        })
        .map_err(|error| ChainError::Rpc(error.to_string()))?;
        Ok(Self {
            client: Tapedrive::new(rpc, payer),
        })
    }
}

impl TapeChain for SdkChain {
    async fn tape_status(&self, tape: Address) -> Result<Option<TapeStatus>, ChainError> {
        match self.client.get_tape(&tape).await {
            Ok(tape) => Ok(Some(TapeStatus {
                capacity: tape.capacity,
                used: tape.used,
                expiry_epoch: tape.expiry_epoch,
                next_track: tape.tracks.next_number(),
                present_tracks: TrackCount(tape.tracks.num_tracks()),
            })),
            Err(TapedriveError::Rpc(RpcError::AccountNotFound(_))) => Ok(None),
            Err(error) => Err(sdk_error(error)),
        }
    }

    async fn current_epoch(&self) -> Result<EpochNumber, ChainError> {
        let system = self
            .client
            .rpc()
            .get_system()
            .await
            .map_err(|error| ChainError::Rpc(error.to_string()))?;
        Ok(system.current_epoch)
    }

    async fn payer_balances(&self) -> Result<PayerBalances, ChainError> {
        let sol = self.client.sol_balance().await.map_err(sdk_error)?;
        let tape = self.client.tape_balance().await.map_err(sdk_error)?;
        Ok(PayerBalances { sol, tape })
    }

    async fn extend_expiry(&self, tape: Address, epochs: EpochNumber) -> Result<(), ChainError> {
        self.client
            .extend_expiry(&tape, epochs.as_u64())
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn extend_capacity(&self, tape: Address, extra: StorageUnits) -> Result<(), ChainError> {
        self.client
            .extend_capacity(&tape, extra)
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn reserve(
        &self,
        key: &TapeKey,
        capacity: StorageUnits,
        epochs: EpochNumber,
    ) -> Result<(), ChainError> {
        self.client
            .reserve(key, capacity, epochs.as_u64())
            .await
            .map_err(sdk_error)?;
        Ok(())
    }
}

impl TrackWriter for SdkChain {
    async fn write_inline(&self, tape: &ListTape, bytes: &[u8]) -> Result<WrittenTrack, ChainError> {
        let key = load_key(&tape.key_path, tape.address).await?;
        let track = self.client.write_raw(&key, bytes).await.map_err(sdk_error)?;
        Ok(WrittenTrack {
            track: track.address(),
            track_number: track.track_number,
        })
    }

    async fn delete_track(&self, tape: &ListTape, track: Address) -> Result<(), ChainError> {
        let key = load_key(&tape.key_path, tape.address).await?;
        match self.client.delete(&key, track).await {
            Ok(()) => Ok(()),
            Err(TapedriveError::NotFound) => Err(ChainError::TrackNotFound(track)),
            Err(error) => Err(sdk_error(error)),
        }
    }

    async fn landed_track(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> Result<Option<CompressedTrack>, ChainError> {
        match self.client.get_track_by_number(&tape, track_number).await {
            Ok(track) => Ok(Some(track)),
            Err(TapedriveError::NotFound) => Ok(None),
            Err(error) => Err(sdk_error(error)),
        }
    }
}

fn sdk_error(error: TapedriveError) -> ChainError {
    ChainError::Sdk(error.to_string())
}
