use anyhow::{Context, Result};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use tape_api::helpers::{build_authority_with_tokens_ix, build_close_ata_ix};
use tape_api::instruction::{
    build_add_to_blacklist_ix, build_advance_pool_ix, build_set_committee_size_ix,
    build_set_spool_groups_ix, build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::{node_pda, stake_pda};
use tape_core::system::{BlacklistEntry, NodeStatus};
use tape_core::types::coin::TAPE;
use tracing::trace;

use crate::fixtures::err::adv_done;
use crate::log::append_log;
use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    const CU_HIGH: u32 = 1_400_000;
    const CU_MED: u32 = 400_000;

    pub fn node_address(&self, index: usize) -> Pubkey {
        let authority = self.harness.nodes()[index].authority();
        let (node_address, _) = node_pda(authority.into());
        node_address.into()
    }

    pub fn stake_address(&self, index: usize) -> Pubkey {
        let authority = self.harness.nodes()[index].authority();
        let (stake_address, _) = stake_pda(authority.into());
        stake_address.into()
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
                payer.pubkey().into(),
                authority.into(),
                amount,
            )?);
        }
        ixs.push(build_stake_with_pool_ix(
            payer.pubkey().into(),
            authority.into(),
            node_address.into(),
            amount,
        ));
        if !payer_is_authority {
            ixs.push(build_close_ata_ix(authority.into(), payer.pubkey().into())?);
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

    pub async fn advance_pool(&self, node_index: usize) -> Result<()> {
        trace!(node_index, "submitting advance_pool instruction");
        let payer = self.harness.admin();
        let current_epoch = self.read_system().await?.current_epoch;
        let node_address = self.node_address(node_index).into();
        let ix = build_advance_pool_ix(
            payer.pubkey().into(),
            node_address,
            current_epoch,
        );
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

    pub async fn add_to_blacklist(
        &self,
        node_index: usize,
        entry: BlacklistEntry,
    ) -> Result<()> {
        trace!(node_index, "submitting add_to_blacklist instruction");
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_add_to_blacklist_ix(
            payer.pubkey().into(),
            node.authority().into(),
            self.node_address(node_index).into(),
            entry,
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
            .with_context(|| format!("add blacklist entry for node {node_index}"))?;

        trace!(node_index, "add_to_blacklist completed");
        Ok(())
    }

    pub async fn set_spool_groups(
        &self,
        node_index: usize,
        spool_groups: u64,
    ) -> Result<()> {
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_set_spool_groups_ix(
            payer.pubkey().into(),
            node.authority().into(),
            self.node_address(node_index).into(),
            spool_groups,
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
            .with_context(|| format!("set spool groups for node {node_index}"))?;

        trace!(
            node_index,
            spool_groups,
            "set_spool_groups completed"
        );
        Ok(())
    }

    pub async fn set_committee_size(
        &self,
        node_index: usize,
        committee_size: u64,
    ) -> Result<()> {
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_set_committee_size_ix(
            payer.pubkey().into(),
            node.authority().into(),
            self.node_address(node_index).into(),
            committee_size,
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
            .with_context(|| format!("set committee size for node {node_index}"))?;

        trace!(
            node_index,
            committee_size,
            "set_committee_size completed"
        );
        Ok(())
    }

    pub fn node_status(&self, index: usize) -> Option<NodeStatus> {
        let ctx = self.harness.nodes()[index].context();
        let state = ctx.state();
        if !state.epoch().is_zero() {
            Some(ctx.node_status())
        } else {
            None
        }
    }

    pub async fn stake_all(&self, amount_tape: u64) -> Result<()> {
        let all: Vec<usize> = (0..self.harness.nodes().len()).collect();
        self.stake_many(&all, amount_tape).await
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

    pub async fn set_spool_groups_many(
        &self,
        node_indices: &[usize],
        spool_groups: u64,
    ) -> Result<()> {
        trace!(
            count = node_indices.len(),
            spool_groups,
            "set_spool_groups_many start"
        );
        append_log(&format!(
            "set spool groups many start count={} spool_groups={spool_groups}",
            node_indices.len()
        ));
        for &i in node_indices {
            self.set_spool_groups(i, spool_groups)
                .await
                .with_context(|| format!("set spool groups for node {i}"))?;
        }
        trace!(
            count = node_indices.len(),
            spool_groups,
            "set_spool_groups_many complete"
        );
        append_log("set spool groups many done");
        Ok(())
    }

    pub async fn set_committee_size_many(
        &self,
        node_indices: &[usize],
        committee_size: u64,
    ) -> Result<()> {
        trace!(
            count = node_indices.len(),
            committee_size,
            "set_committee_size_many start"
        );
        append_log(&format!(
            "set committee size many start count={} committee_size={committee_size}",
            node_indices.len()
        ));
        for &i in node_indices {
            self.set_committee_size(i, committee_size)
                .await
                .with_context(|| format!("set committee size for node {i}"))?;
        }
        trace!(
            count = node_indices.len(),
            committee_size,
            "set_committee_size_many complete"
        );
        append_log("set committee size many done");
        Ok(())
    }
}
