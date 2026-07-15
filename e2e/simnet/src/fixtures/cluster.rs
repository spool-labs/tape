use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tape_core::types::BasisPoints;
use tracing::trace;

use crate::gateway::TestGateway;
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
        commission: BasisPoints,
        stake_amount_tape: u64,
        health_timeout: Duration,
    ) -> Result<()> {
        trace!(
            nodes = self.config().node_count,
            stake_amount_tape,
            commission = ?commission,
            "bootstrap_nodes start"
        );
        {
            let scenario = self.scenario();
            scenario.init_system().await.context("init_system")?;
            scenario
                .register_nodes(commission)
                .await
                .context("register_nodes")?;
            scenario
                .stake_all(stake_amount_tape)
                .await
                .context("stake_all")?;
            scenario.start_network().await.context("start_network")?;
        }

        self.start_all_with_retry(START_TRIES, Duration::from_millis(START_DELAY_MS))
            .await?;

        let scenario = self.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .context("wait_nodes_healthy")?;

        trace!("bootstrap_nodes complete");
        Ok(())
    }

    /// Wait until every running storage node has the gateway as a known peer.
    pub async fn wait_gateway_known(
        &self,
        gateway: &TestGateway,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let tls_pubkey = gateway.tls_pubkey();

        loop {
            let mut running = 0usize;
            let mut known = 0usize;
            for node in self.nodes().iter().filter(|node| node.is_running()) {
                running += 1;
                if node
                    .context()
                    .peer_manager
                    .peer_for_tls_pubkey(tls_pubkey)
                    .is_some()
                {
                    known += 1;
                }
            }

            if running > 0 && known == running {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!(
                    "timed out waiting for storage nodes to learn gateway peer, known {known}/{running}"
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
