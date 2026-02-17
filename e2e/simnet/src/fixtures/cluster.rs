use std::time::Duration;

use anyhow::{bail, Context, Result};
use tape_core::types::BasisPoints;

use crate::simnet::SimnetHarness;

const START_TRIES: usize = 3;
const START_DELAY_MS: u64 = 200;

impl SimnetHarness {
    pub async fn start_all_with_retry(&mut self, tries: usize, delay: Duration) -> Result<()> {
        let tries = tries.max(1);

        for _ in 0..tries {
            self.start_all().await?;
            if self.nodes().iter().all(|n| n.is_running()) {
                return Ok(());
            }
            tokio::time::sleep(delay).await;
        }

        let failed: Vec<_> = self
            .nodes()
            .iter()
            .filter(|n| !n.is_running())
            .map(|n| n.id())
            .collect();
        bail!("failed to start runtime on nodes: {failed:?}");
    }

    /// Initialize chain state and bootstrap all configured nodes to joined state.
    pub async fn bootstrap_nodes(
        &mut self,
        payer_index: usize,
        commission: BasisPoints,
        stake_amount_tape: u64,
        health_timeout: Duration,
    ) -> Result<()> {
        {
            let scenario = self.scenario();
            scenario.init_system(payer_index).await.context("init_system")?;
            scenario
                .register_nodes(commission)
                .await
                .context("register_nodes")?;
            scenario
                .stake_all(payer_index, stake_amount_tape)
                .await
                .context("stake_all")?;
            scenario.pool_all(payer_index).await.context("pool_all")?;
            scenario.join_all(payer_index).await.context("join_all")?;
        }

        self.start_all_with_retry(START_TRIES, Duration::from_millis(START_DELAY_MS))
            .await?;

        let scenario = self.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .context("wait_nodes_healthy")?;

        Ok(())
    }
}
