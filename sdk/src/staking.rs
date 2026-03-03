//! Staking operations for the Tapedrive network.

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;

use rpc_client::Rpc;
use tape_api::compute::{
    ADVANCE_POOL_CU, REQUEST_STAKE_UNLOCK_CU, STAKE_WITH_POOL_CU, UNSTAKE_FROM_POOL_CU,
};
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_advance_pool_ix, build_request_stake_unlock_ix, build_stake_with_pool_ix,
    build_unstake_from_pool_ix,
};
use tape_core::types::coin::{Coin, TAPE};

use crate::error::TapedriveError;
use crate::stake_key::StakeKey;
use crate::tapedrive::Tapedrive;

impl<R: Rpc> Tapedrive<R> {
    /// Delegate TAPE to a node's staking pool.
    ///
    /// Creates an ATA for the stake authority, transfers TAPE from the payer,
    /// and stakes with the specified pool. The ATA is left open.
    pub async fn stake_with_pool(
        &self,
        stake_key: &StakeKey,
        pool: Pubkey,
        amount: Coin<TAPE>,
    ) -> Result<(), TapedriveError> {
        let mut ixs = vec![ComputeBudgetInstruction::set_compute_unit_limit(
            STAKE_WITH_POOL_CU,
        )];
        ixs.extend(build_authority_with_tokens_ix(
            self.payer.pubkey(),
            stake_key.pubkey(),
            amount,
        ));
        ixs.push(build_stake_with_pool_ix(
            self.payer.pubkey(),
            stake_key.pubkey(),
            pool,
            amount,
        ));

        self.client
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[stake_key.as_keypair()],
            )
            .await?;

        Ok(())
    }

    /// Advance a node's staking pool to the current epoch.
    ///
    /// Permissionless — only the payer signs. `node_authority` is passed as a
    /// non-signer account reference.
    pub async fn advance_pool(
        &self,
        node_authority: Pubkey,
        pool: Pubkey,
    ) -> Result<(), TapedriveError> {
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(ADVANCE_POOL_CU);
        let ix = build_advance_pool_ix(self.payer.pubkey(), node_authority, pool);
        self.client
            .send_instructions(&self.payer, vec![cu_ix, ix])
            .await?;
        Ok(())
    }

    /// Request unlock of a delegated stake from a pool.
    ///
    /// The stake authority must sign. After the unlock delay, call
    /// [`unstake_from_pool`](Tapedrive::unstake_from_pool) to withdraw.
    pub async fn request_stake_unlock(
        &self,
        stake_key: &StakeKey,
        pool: Pubkey,
    ) -> Result<(), TapedriveError> {
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(REQUEST_STAKE_UNLOCK_CU);
        let ix = build_request_stake_unlock_ix(
            self.payer.pubkey(),
            stake_key.pubkey(),
            pool,
        );
        self.client
            .send_instructions_with_signers(
                &self.payer,
                vec![cu_ix, ix],
                &[stake_key.as_keypair()],
            )
            .await?;
        Ok(())
    }

    /// Withdraw a previously unlocked stake from a pool.
    ///
    /// The stake authority must sign. Tokens return to the authority's ATA.
    pub async fn unstake_from_pool(
        &self,
        stake_key: &StakeKey,
        pool: Pubkey,
    ) -> Result<(), TapedriveError> {
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(UNSTAKE_FROM_POOL_CU);
        let ix = build_unstake_from_pool_ix(
            self.payer.pubkey(),
            stake_key.pubkey(),
            pool,
        );
        self.client
            .send_instructions_with_signers(
                &self.payer,
                vec![cu_ix, ix],
                &[stake_key.as_keypair()],
            )
            .await?;
        Ok(())
    }
}
