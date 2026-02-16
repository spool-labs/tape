use anyhow::{Context, Result};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signer;
use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_advance_pool_ix, build_claim_commission_ix, build_epoch_sync_ix, build_join_network_ix,
    build_request_stake_unlock_ix, build_set_commission_ix, build_stake_with_pool_ix,
    build_unstake_from_pool_ix,
};
use tape_api::program::tapedrive::{node_pda, stake_pda};
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

use crate::harness::fixture::err::{adv_done, join_done, sync_done};
use crate::harness::fixture::network::SimNet;
use crate::harness::log::append_log;

impl SimNet {
    const CU_HIGH: u32 = 1_400_000;
    const CU_MED: u32 = 400_000;

    pub fn node_address(&self, index: usize) -> solana_sdk::pubkey::Pubkey {
        let authority = self.nodes[index].ctx.pubkey();
        let (node_address, _) = node_pda(authority);
        node_address
    }

    pub fn stake_address(&self, index: usize) -> solana_sdk::pubkey::Pubkey {
        let authority = self.nodes[index].ctx.pubkey();
        let (stake_address, _) = stake_pda(authority);
        stake_address
    }

    pub fn fund_node(&self, index: usize, lamports: u64) -> Result<()> {
        let authority = self.nodes[index].ctx.pubkey();
        self.rpc
            .airdrop(&authority, lamports)
            .with_context(|| format!("airdrop node {}", index))
    }

    pub async fn stake_node(&self, index: usize, amount_tape: u64) -> Result<solana_sdk::pubkey::Pubkey> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let amount = TAPE::parse(&amount_tape.to_string())
            .map_err(|_| anyhow::anyhow!("invalid stake amount"))?;

        let mut instructions = build_authority_with_tokens_ix(self.payer.pubkey(), authority, amount);
        instructions.insert(
            0,
            ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_HIGH),
        );
        instructions.push(build_stake_with_pool_ix(
            self.payer.pubkey(),
            authority,
            node_address,
            amount,
        ));
        instructions.push(build_close_ata_ix(authority, self.payer.pubkey()));

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                instructions,
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("stake node {}", index))?;

        Ok(self.stake_address(index))
    }

    pub async fn unlock_stake(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_request_stake_unlock_ix(self.payer.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("unlock stake for node {}", index))?;

        Ok(())
    }

    pub async fn withdraw_stake(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_unstake_from_pool_ix(self.payer.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("withdraw stake for node {}", index))?;

        Ok(())
    }

    pub async fn join_node(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_join_network_ix(self.payer.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("join node {}", index))?;

        Ok(())
    }

    pub async fn join_node_ok(&self, index: usize) -> Result<()> {
        if let Err(error) = self.join_node(index).await {
            if !join_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn advance_pool(&self, index: usize) -> Result<()> {
        let authority = self.nodes[index].ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_advance_pool_ix(self.payer.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions(self.payer.as_ref(), vec![cu_ix, instruction])
            .await
            .with_context(|| format!("advance pool for node {}", index))?;

        Ok(())
    }

    pub async fn advance_pool_ok(&self, index: usize) -> Result<()> {
        if let Err(error) = self.advance_pool(index).await {
            if !adv_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn sync_node(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);

        let epoch = self.client.get_epoch().await.context("read epoch for sync")?;
        let spools = node.ctx.control_plane.get_our_spools();
        let instruction = build_epoch_sync_ix(
            self.payer.pubkey(),
            authority,
            node_address,
            EpochNumber(epoch.id.as_u64()),
            &spools,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("sync node {}", index))?;

        Ok(())
    }

    pub async fn sync_node_ok(&self, index: usize) -> Result<()> {
        if let Err(error) = self.sync_node(index).await {
            if !sync_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn set_commission(&self, index: usize, basis_points: u64) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_set_commission_ix(
            self.payer.pubkey(),
            authority,
            node_address,
            BasisPoints(basis_points),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("set commission for node {}", index))?;

        Ok(())
    }

    pub async fn claim_commission(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let authority = node.ctx.pubkey();
        let node_address = self.node_address(index);
        let instruction = build_claim_commission_ix(self.payer.pubkey(), authority, node_address);
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.client
            .send_instructions_with_signers(
                self.payer.as_ref(),
                vec![cu_ix, instruction],
                &[node.ctx.keypair.as_ref()],
            )
            .await
            .with_context(|| format!("claim commission for node {}", index))?;

        Ok(())
    }

    pub fn node_status(&self, index: usize) -> Option<NodeStatus> {
        self.nodes[index].ctx.storage.store.get_node_status().ok().flatten()
    }

    pub async fn stake_all(&self, amount_tape: u64) -> Result<()> {
        append_log(&format!("stake all start count={} amount={amount_tape}", self.nodes.len()));
        for index in 0..self.nodes.len() {
            self.stake_node(index, amount_tape)
                .await
                .with_context(|| format!("stake node {}", index))?;
        }
        append_log("stake all done");
        Ok(())
    }

    pub async fn join_all(&self) -> Result<()> {
        append_log(&format!("join all start count={}", self.nodes.len()));
        for index in 0..self.nodes.len() {
            self.join_node_ok(index)
                .await
                .with_context(|| format!("join node {}", index))?;
        }
        append_log("join all done");
        Ok(())
    }

    pub async fn pool_all(&self) -> Result<()> {
        append_log(&format!("pool all start count={}", self.nodes.len()));
        for index in 0..self.nodes.len() {
            self.advance_pool_ok(index)
                .await
                .with_context(|| format!("advance pool for node {}", index))?;
        }
        append_log("pool all done");
        Ok(())
    }
}
