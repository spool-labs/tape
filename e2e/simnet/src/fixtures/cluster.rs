use std::time::{Duration, Instant};

use peer_manager::PeerManager;
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
        let tls_pubkey = gateway.tls_pubkey();
        self.wait_peers(timeout, "gateway peer known", move |peers| {
            peers.peer_for_tls_pubkey(tls_pubkey).is_some()
        })
        .await
    }

    /// Wait until every running storage node's peer cache satisfies the
    /// predicate.
    pub async fn wait_peers(
        &self,
        timeout: Duration,
        what: &str,
        check: impl Fn(&PeerManager) -> bool,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let mut running = 0usize;
            let mut satisfied = 0usize;
            for node in self.nodes().iter().filter(|node| node.is_running()) {
                running += 1;
                if check(&node.context().peer_manager) {
                    satisfied += 1;
                }
            }

            self.check_cluster_alive(what)?;
            if satisfied == running {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for {what}: {satisfied}/{running} nodes");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Wait until every running node has ingested everything submitted so far.
    ///
    /// A transaction is recorded in the slot after the confirmed tip and stays
    /// invisible until the producer closes that slot, so the barrier is the
    /// tip plus one.
    pub async fn wait_ingest_current(&self, timeout: Duration) -> Result<()> {
        let tip = self.chain().current_slot().await.context("current_slot")?;
        self.wait_ingest_slot(tip + 1, timeout).await
    }

    /// Wait until every running node has dispatched the block at `slot`.
    ///
    /// Peers, tracks and tape metadata reach a node only when the ingestor
    /// promotes the block carrying them, so a test that submits a transaction
    /// and then reads node state has to wait for that slot. The at tip check
    /// will not do: it tolerates a few slots of lag, which is the exact
    /// window a just submitted transaction sits in.
    pub async fn wait_ingest_slot(&self, slot: u64, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let mut running = 0usize;
            let mut caught_up = 0usize;
            let mut slowest = u64::MAX;
            for node in self.nodes().iter().filter(|node| node.is_running()) {
                running += 1;
                let dispatched = node.context().ingest.progress().last_dispatched_slot();
                slowest = slowest.min(dispatched);
                if dispatched >= slot {
                    caught_up += 1;
                }
            }

            self.check_cluster_alive("ingest progress")?;
            if caught_up == running {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!(
                    "timed out waiting for ingest through slot {slot}:                      {caught_up}/{running} nodes, slowest at {slowest}"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// A stopped cluster is not a slow one. Node runtimes exit on their own
    /// only on a service failure or a process signal, so a wait that keeps
    /// polling a stopped cluster reports the timeout instead of the cause.
    fn check_cluster_alive(&self, what: &str) -> Result<()> {
        if self.nodes().iter().any(|node| node.is_running()) {
            return Ok(());
        }
        bail!(
            "{what}: all {} node runtimes have exited; check target/sim-e2e/sim.log for              service failures or shutdown signals",
            self.nodes().len()
        );
    }
}
