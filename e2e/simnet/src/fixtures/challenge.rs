use anyhow::{Context, Result};
use solana_signer::Signer;
use tape_api::instruction::build_set_challenge_cadence_ix;
use tape_core::types::SlotNumber;
use tape_node::features::challenge::board::BoardRound;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    const CADENCE_CU: u32 = 400_000;

    /// Copy the rounds a node has on its board after `since`, oldest first.
    pub fn challenge_rounds(&self, index: usize, since: SlotNumber) -> Vec<BoardRound> {
        self.harness.nodes()[index]
            .context()
            .challenge
            .rounds_since(since, usize::MAX)
    }

    /// Vote one node's challenge cadence, in slots.
    pub async fn set_challenge_cadence(&self, node_index: usize, slots: u64) -> Result<()> {
        let payer = self.harness.admin();
        let node = &self.harness.nodes()[node_index];

        let ix = build_set_challenge_cadence_ix(
            payer.pubkey().into(),
            node.authority().into(),
            self.node_address(node_index).into(),
            slots,
        );

        self.harness
            .chain()
            .send_capped_with_signers_and_advance(
                payer,
                Self::CADENCE_CU,
                vec![ix],
                &[node.keypair()],
                self.harness.config().slot_advance_per_tx,
            )
            .await
            .with_context(|| format!("set challenge cadence for node {node_index}"))?;

        Ok(())
    }

    pub async fn set_challenge_cadence_many(&self, node_indices: &[usize], slots: u64) -> Result<()> {
        for &index in node_indices {
            self.set_challenge_cadence(index, slots).await?;
        }
        Ok(())
    }
}
