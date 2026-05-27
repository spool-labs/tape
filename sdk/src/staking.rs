//! Staking operations for the Tapedrive network.

use rpc::Rpc;
use rpc_client::parse_tape_error;
use tape_api::compute::{
    ADVANCE_POOL_CU, REQUEST_STAKE_UNLOCK_CU, STAKE_WITH_POOL_CU, UNSTAKE_FROM_POOL_CU,
};
use tape_api::errors::TapeError;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_advance_pool_ix, build_request_stake_unlock_ix, build_stake_with_pool_ix,
    build_unstake_from_pool_ix,
};
use tape_api::program::tapedrive::{history_pda, track_pda};
use tape_core::staking::{PoolRate, RateSpan};
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_retry::{retry_if, RetryConfig, Retryable};

use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::stake_key::StakeKey;
use crate::tapedrive::Tapedrive;
use crate::track::query_track_proof;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Delegate TAPE to a node's staking pool.
    ///
    /// Creates an ATA for the stake authority, transfers TAPE from the payer,
    /// and stakes with the specified pool. The ATA is left open.
    pub async fn stake_with_pool(
        &self,
        stake_key: &StakeKey,
        pool: Address,
        amount: Coin<TAPE>,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let stake_signer = stake_key.keypair();

        let mut ixs = Vec::new();

        ixs.extend(build_authority_with_tokens_ix(
            payer.pubkey().into(),
            stake_key.pubkey().into(),
            amount,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?);

        ixs.push(
            build_stake_with_pool_ix(
                payer.pubkey().into(),
                stake_key.pubkey().into(),
                pool,
                amount,
            ),
        );

        self.rpc()
            .send_instructions_with_signers_and_compute_unit_limit(
                payer,
                STAKE_WITH_POOL_CU,
                ixs,
                &[stake_signer],
            )
            .await?;

        Ok(())
    }

    /// Advance a node's staking pool to the current epoch.
    pub async fn advance_pool(&self, pool: Address) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let current_epoch = self.rpc().get_system().await?.current_epoch;
        let ix = build_advance_pool_ix(payer.pubkey().into(), pool, current_epoch);

        self.rpc()
            .send_instructions_with_compute_unit_limit(payer, ADVANCE_POOL_CU, vec![ix])
            .await?;

        Ok(())
    }

    /// Request unlock of a delegated stake from a pool.
    pub async fn request_stake_unlock(
        &self,
        stake_key: &StakeKey,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let stake_signer = stake_key.keypair();
        let stake = self.rpc().get_stake(&stake_key.pubkey().into()).await?;
        let pool = stake.pool;

        retry_if(
            RetryConfig::three(),
            None,
            || async {
                let pool_rate = self
                    .resolve_pool_rate(pool, stake.inner.activation_epoch.prev())
                    .await?;

                let ix = build_request_stake_unlock_ix(
                    payer.pubkey().into(),
                    stake_key.pubkey().into(),
                    pool,
                    pool_rate,
                );

                self.rpc()
                    .send_instructions_with_signers_and_compute_unit_limit(
                        payer,
                        REQUEST_STAKE_UNLOCK_CU,
                        vec![ix],
                        &[stake_signer],
                    )
                    .await
                    .map_err(TapedriveError::Rpc)?;
                Ok(())
            },
            should_retry_pool_rate,
        )
        .await
    }

    /// Withdraw a previously unlocked stake from a pool.
    pub async fn unstake_from_pool(
        &self,
        stake_key: &StakeKey,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let stake_signer = stake_key.keypair();
        let stake = self.rpc().get_stake(&stake_key.pubkey().into()).await?;
        let pool = stake.pool;
        let withdraw_epoch = stake
            .inner
            .withdraw_epoch()
            .ok_or_else(|| TapedriveError::InvalidArgument("stake is not unlocking".into()))?;

        retry_if(
            RetryConfig::three(),
            None,
            || async {
                let pool_rate = self.resolve_pool_rate(pool, withdraw_epoch.prev()).await?;

                let ix = build_unstake_from_pool_ix(
                    payer.pubkey().into(),
                    stake_key.pubkey().into(),
                    pool,
                    pool_rate,
                );

                self.rpc()
                    .send_instructions_with_signers_and_compute_unit_limit(
                        payer,
                        UNSTAKE_FROM_POOL_CU,
                        vec![ix],
                        &[stake_signer],
                    )
                    .await
                    .map_err(TapedriveError::Rpc)?;
                Ok(())
            },
            should_retry_pool_rate,
        )
        .await
    }

    async fn resolve_pool_rate(
        &self,
        pool: Address,
        target_epoch: EpochNumber,
    ) -> Result<PoolRate, TapedriveError> {
        let (history_tape, _) = history_pda(pool);
        let mut cursor: Option<TrackNumber> = None;

        loop {
            let (tracks, next_cursor) = self
                .list_tracks_by_tape(&history_tape, cursor, 128)
                .await?;

            for track in tracks {
                let (track_address, _) = track_pda(history_tape, track.track_number);
                let data = match self.read(&track_address).await {
                    Ok(data) => data,
                    Err(TapedriveError::NotFound) => continue,
                    Err(error) => return Err(error),
                };
                if data.len() != core::mem::size_of::<RateSpan>() {
                    continue;
                }

                let span = bytemuck::try_from_bytes::<RateSpan>(&data)
                    .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

                if span.node != pool || !span.contains(target_epoch) {
                    continue;
                }

                let proof = query_track_proof(self, &track_address).await?;
                return Ok(PoolRate::new(*span, proof));
            }

            match next_cursor {
                Some(next) => cursor = Some(next),
                None => return Err(TapedriveError::NotFound),
            }
        }
    }
}

fn should_retry_pool_rate(err: &TapedriveError) -> bool {
    match err {
        TapedriveError::NotFound => true,
        TapedriveError::Rpc(err) => {
            matches!(parse_tape_error(err), Some(TapeError::RateMissing | TapeError::BadProof))
        }
        TapedriveError::Peer(err) => err.is_retryable(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use tape_protocol::api::ApiError;

    use super::*;

    #[test]
    fn pool_rate_retries_stale_track_proof() {
        assert!(should_retry_pool_rate(&TapedriveError::Peer(
            ApiError::StaleTrackProof,
        )));
    }
}
