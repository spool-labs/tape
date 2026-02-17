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
use tape_store::ops::{MetaOps, SpoolOps};
use tape_store::types::NodeStatus;

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
        payer_index: usize,
        node_index: usize,
        amount_tape: u64,
    ) -> Result<Pubkey> {
        let payer = self.harness.nodes()[payer_index].keypair();
        let node = &self.harness.nodes()[node_index];

        let authority = node.authority();
        let node_address = self.node_address(node_index);
        let amount = TAPE::parse(&amount_tape.to_string())
            .map_err(|_| anyhow::anyhow!("invalid stake amount"))?;

        let mut ixs = build_authority_with_tokens_ix(payer.pubkey(), authority, amount);
        ixs.insert(
            0,
            ComputeBudgetInstruction::set_compute_unit_limit(Self::CU_HIGH),
        );
        ixs.push(build_stake_with_pool_ix(
            payer.pubkey(),
            authority,
            node_address,
            amount,
        ));
        ixs.push(build_close_ata_ix(authority, payer.pubkey()));

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

        Ok(self.stake_address(node_index))
    }

    pub async fn unlock_stake(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn withdraw_stake(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn join_node(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn join_node_ok(&self, payer_index: usize, node_index: usize) -> Result<()> {
        if let Err(error) = self.join_node(payer_index, node_index).await {
            if !join_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn advance_pool(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn advance_pool_ok(&self, payer_index: usize, node_index: usize) -> Result<()> {
        if let Err(error) = self.advance_pool(payer_index, node_index).await {
            if !adv_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn sync_node(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn sync_node_ok(&self, payer_index: usize, node_index: usize) -> Result<()> {
        if let Err(error) = self.sync_node(payer_index, node_index).await {
            if !sync_done(&error) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn set_commission(
        &self,
        payer_index: usize,
        node_index: usize,
        basis_points: u64,
    ) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub async fn claim_commission(&self, payer_index: usize, node_index: usize) -> Result<()> {
        let payer = self.harness.nodes()[payer_index].keypair();
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

        Ok(())
    }

    pub fn node_status(&self, index: usize) -> Option<NodeStatus> {
        self.harness.nodes()[index]
            .context()
            .store
            .get_node_status()
            .ok()
            .flatten()
    }

    pub async fn stake_all(&self, payer_index: usize, amount_tape: u64) -> Result<()> {
        append_log(&format!(
            "stake all start count={} amount={amount_tape}",
            self.harness.nodes().len()
        ));
        for i in 0..self.harness.nodes().len() {
            self.stake_node(payer_index, i, amount_tape)
                .await
                .with_context(|| format!("stake node {i}"))?;
        }
        append_log("stake all done");
        Ok(())
    }

    pub async fn join_all(&self, payer_index: usize) -> Result<()> {
        append_log(&format!("join all start count={}", self.harness.nodes().len()));
        for i in 0..self.harness.nodes().len() {
            self.join_node_ok(payer_index, i)
                .await
                .with_context(|| format!("join node {i}"))?;
        }
        append_log("join all done");
        Ok(())
    }

    pub async fn pool_all(&self, payer_index: usize) -> Result<()> {
        append_log(&format!("pool all start count={}", self.harness.nodes().len()));
        for i in 0..self.harness.nodes().len() {
            self.advance_pool_ok(payer_index, i)
                .await
                .with_context(|| format!("advance pool for node {i}"))?;
        }
        append_log("pool all done");
        Ok(())
    }
}
