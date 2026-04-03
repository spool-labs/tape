//! Staking operations for the Tapedrive network.

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use rpc::Rpc;
use tape_api::compute::{
    ADVANCE_POOL_CU, REQUEST_STAKE_UNLOCK_CU, STAKE_WITH_POOL_CU, UNSTAKE_FROM_POOL_CU,
};
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_advance_pool_ix, build_request_stake_unlock_ix, build_stake_with_pool_ix,
    build_unstake_from_pool_ix,
};
use tape_core::types::coin::{Coin, TAPE};
use tape_crypto::address::Address;

use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::stake_key::StakeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> 
    Tapedrive<Blockchain, Cluster> {

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

        let mut ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(STAKE_WITH_POOL_CU),
        ];

        ixs.extend(build_authority_with_tokens_ix(
            payer.pubkey().into(),
            stake_key.address(),
            amount,
        )
        .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?);

        ixs.push(
            build_stake_with_pool_ix(
                payer.pubkey().into(),
                stake_key.address(),
                pool,
                amount,
            ),
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                ixs,
                &[stake_signer],
            )
            .await?;

        Ok(())
    }

    /// Advance a node's staking pool to the current epoch.
    pub async fn advance_pool(
        &self,
        node_authority: Address,
        pool: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
            ADVANCE_POOL_CU);

        let ix = build_advance_pool_ix(payer.pubkey().into(), node_authority, pool);

        self.rpc()
            .send_instructions(payer, vec![cu_ix, ix])
            .await?;

        Ok(())
    }

    /// Request unlock of a delegated stake from a pool.
    pub async fn request_stake_unlock(
        &self,
        stake_key: &StakeKey,
        pool: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let stake_signer = stake_key.keypair();
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(REQUEST_STAKE_UNLOCK_CU);

        let ix = build_request_stake_unlock_ix(
            payer.pubkey().into(),
            stake_key.address(),
            pool,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![cu_ix, ix],
                &[stake_signer],
            )
            .await?;

        Ok(())
    }

    /// Withdraw a previously unlocked stake from a pool.
    pub async fn unstake_from_pool(
        &self,
        stake_key: &StakeKey,
        pool: Address,
    ) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let stake_signer = stake_key.keypair();
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(UNSTAKE_FROM_POOL_CU);

        let ix = build_unstake_from_pool_ix(
            payer.pubkey().into(),
            stake_key.address(),
            pool,
        );

        self.rpc()
            .send_instructions_with_signers(
                payer,
                vec![cu_ix, ix],
                &[stake_signer],
            )
            .await?;

        Ok(())
    }
}
