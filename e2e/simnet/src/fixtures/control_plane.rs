use anyhow::{Context, Result};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_advance_pool_ix, build_claim_commission_ix, build_epoch_sync_ix, build_join_network_ix,
    build_request_stake_unlock_ix, build_set_commission_ix, build_stake_with_pool_ix,
    build_unstake_from_pool_ix,
};
use tape_api::program::tapedrive::{node_pda, stake_pda};
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_store::ops::SpoolOps;
use tape_store::types::NodeStatus;
use tracing::trace;

use crate::fixtures::err::{adv_done, join_done, sync_done};
use crate::log::append_log;
use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    const CU_HIGH: u32 = 1_400_000;
    const CU_MED: u32 = 400_000;

    pub fn node_address(&self, index: usize) -> Pubkey {
        let authority = self.harness.nodes()[index].authority();
        let (node_address, _) = node_pda(authority);
        node_address
    }

    pub fn stake_address(&self, index: usize) -> Pubkey {
        let authority = self.harness.nodes()[index].authority();
        let (stake_address, _) = stake_pda(authority);
        stake_address
    }

    pub fn fund_node(&self, index: usize, lamports: u64) -> Result<()> {
        let authority = self.harness.nodes()[index].authority();
        self.harness
            .chain()
            .airdrop(&authority, lamports)
            .with_context(|| format!("airdrop node {index}"))
    }

    pub async fn stake_node(
        &self,
        node_index: usize,
        amount_tape: u64,
    ) -> Result<Pubkey> {
        trace!(
            node_index,
            amount_tape,
            "submitting stake_node instruction"
        );
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let authority = node.authority();
        let node_address = self.node_address(node_index);
        let amount = TAPE::parse(&amount_tape.to_string())
            .map_err(|_| anyhow::anyhow!("invalid stake amount"))?;

        let mut ixs = vec![ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_HIGH)];
        let payer_is_authority = payer.pubkey() == authority;
        if !payer_is_authority {
            ixs.extend(build_authority_with_tokens_ix(
                payer.pubkey(),
                authority,
                amount,
            ));
        }
        ixs.push(build_stake_with_pool_ix(
            payer.pubkey(),
            authority,
            node_address,
            amount,
        ));
        if !payer_is_authority {
            ixs.push(build_close_ata_ix(authority, payer.pubkey()));
        }

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                ixs,
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("stake node {node_index}"))?;

        trace!(node_index, "stake_node completed");
        Ok(self.stake_address(node_index))
    }

    pub async fn unlock_stake(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting unlock_stake instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_request_stake_unlock_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("unlock stake for node {node_index}"))?;

        trace!(node_index, "unlock_stake completed");
        Ok(())
    }

    pub async fn withdraw_stake(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting withdraw_stake instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_unstake_from_pool_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("withdraw stake for node {node_index}"))?;

        trace!(node_index, "withdraw_stake completed");
        Ok(())
    }

    pub async fn join_node(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting join_network instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_join_network_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("join node {node_index}"))?;

        trace!(node_index, "join_node completed");
        Ok(())
    }

    pub async fn join_node_ok(&self, node_index: usize) -> Result<()> {
        if let Err(error) = self.join_node(node_index).await {
            if !join_done(&error) {
                return Err(error);
            }
            trace!(node_index, "join_node idempotent completion");
        }
        trace!(node_index, "join_node_ok complete");
        Ok(())
    }

    pub async fn advance_pool(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting advance_pool instruction");
        let payer = self.harness.admin();
        let authority = self.harness.nodes()[node_index].authority();
        let ix = build_advance_pool_ix(payer.pubkey(), authority, self.node_address(node_index));
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_and_advance(
                payer,
                vec![cu_ix, ix],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("advance pool for node {node_index}"))?;

        trace!(node_index, "advance_pool completed");
        Ok(())
    }

    pub async fn advance_pool_ok(&self, node_index: usize) -> Result<()> {
        if let Err(error) = self.advance_pool(node_index).await {
            if !adv_done(&error) {
                return Err(error);
            }
            trace!(node_index, "advance_pool idempotent completion");
        }
        trace!(node_index, "advance_pool_ok complete");
        Ok(())
    }

    pub async fn sync_node(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting sync_network instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let client = rpc_client::RpcClient::from_rpc(self.harness.chain().rpc().clone());
        let epoch = client.get_epoch().await.context("read epoch for sync")?;

        let spools: Vec<u16> = node
            .context()
            .store
            .iter_all_spools()
            .with_context(|| format!("read spools for node {node_index}"))?
            .into_iter()
            .map(|(spool_id, _)| spool_id)
            .collect();

        let ix = build_epoch_sync_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
            EpochNumber(epoch.id.as_u64()),
            &spools,
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("sync node {node_index}"))?;

        trace!(node_index, "sync_node completed");
        Ok(())
    }

    pub async fn sync_node_ok(&self, node_index: usize) -> Result<()> {
        if let Err(error) = self.sync_node(node_index).await {
            if !sync_done(&error) {
                return Err(error);
            }
            trace!(node_index, "sync_node idempotent completion");
        }
        trace!(node_index, "sync_node_ok complete");
        Ok(())
    }

    pub async fn set_commission(
        &self,
        node_index: usize,
        basis_points: u64,
    ) -> Result<()> {
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_set_commission_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
            BasisPoints(basis_points),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("set commission for node {node_index}"))?;

        trace!(
            node_index,
            basis_points,
            "set_commission completed"
        );
        Ok(())
    }

    pub async fn claim_commission(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting claim_commission instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_claim_commission_ix(
            payer.pubkey(),
            node.authority(),
            self.node_address(node_index),
        );
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_MED);

        self.harness
            .chain()
            .send_instructions_with_signers_and_advance(
                payer,
                vec![cu_ix, ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("claim commission for node {node_index}"))?;

        trace!(node_index, "claim_commission completed");
        Ok(())
    }

    pub fn node_status(&self, index: usize) -> Option<NodeStatus> {
        let cs = self.harness.nodes()[index].context().chain_state.load();
        if cs.has_epoch() {
            Some(cs.node_status.clone())
        } else {
            None
        }
    }

    pub async fn stake_all(&self, amount_tape: u64) -> Result<()> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.stake_many(&all, amount_tape).await
    }

    pub async fn join_all(&self) -> Result<()> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.join_many(&all).await
    }

    pub async fn pool_all(&self) -> Result<()> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.pool_many(&all).await
    }

    pub async fn stake_many(
        &self,
        node_indices: &[usize],
        amount_tape: u64,
    ) -> Result<()> {
        trace!(
            count = node_indices.len(),
            amount_tape,
            "stake_many start"
        );
        append_log(&format!(
            "stake many start count={} amount={amount_tape}",
            node_indices.len()
        ));
        for &i in node_indices {
            self.stake_node(i, amount_tape)
                .await
                .with_context(|| format!("stake node {i}"))?;
        }
        trace!(
            count = node_indices.len(),
            "stake_many complete"
        );
        append_log("stake many done");
        Ok(())
    }

    pub async fn join_many(&self, node_indices: &[usize]) -> Result<()> {
        trace!(count = node_indices.len(), "join_many start");
        append_log(&format!("join many start count={}", node_indices.len()));
        for &i in node_indices {
            self.join_node_ok(i)
                .await
                .with_context(|| format!("join node {i}"))?;
        }
        trace!(count = node_indices.len(), "join_many complete");
        append_log("join many done");
        Ok(())
    }

    pub async fn pool_many(&self, node_indices: &[usize]) -> Result<()> {
        trace!(count = node_indices.len(), "pool_many start");
        append_log(&format!("pool many start count={}", node_indices.len()));
        for &i in node_indices {
            self.advance_pool_ok(i)
                .await
                .with_context(|| format!("advance pool for node {i}"))?;
        }
        trace!(count = node_indices.len(), "pool_many complete");
        append_log("pool many done");
        Ok(())
    }
}
