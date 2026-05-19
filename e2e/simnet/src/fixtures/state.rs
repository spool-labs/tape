use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rpc_client::RpcClient;
use tape_api::prelude::{Archive, Epoch, System};
use tape_api::program::MIN_COMMITTEE_SIZE;
use tape_core::types::EpochNumber;
use tape_protocol::fetch::fetch_state;
use tracing::trace;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    pub async fn read_system(&self) -> Result<System> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_system().await.context("read system")
    }

    pub async fn read_epoch(&self) -> Result<Epoch> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        let system = client.get_system().await.context("read system for epoch")?;
        client
            .get_epoch(system.current_epoch)
            .await
            .context("read epoch")
    }

    pub async fn read_archive(&self) -> Result<Archive> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_archive().await.context("read archive")
    }

    pub async fn committee_size(&self) -> Result<usize> {
        let system = self.read_system().await?;
        self.committee_len(system.current_epoch).await
    }

    pub async fn committee_next_size(&self) -> Result<usize> {
        let system = self.read_system().await?;
        self.committee_len(system.current_epoch + EpochNumber(1)).await
    }

    pub async fn is_bootstrap_mode(&self) -> Result<bool> {
        let system = self.read_system().await?;
        Ok(self
            .committee_len(system.current_epoch.saturating_sub(EpochNumber(1)))
            .await?
            == 0)
    }

    pub async fn is_low_quorum(&self) -> Result<bool> {
        Ok(self.committee_size().await? < MIN_COMMITTEE_SIZE)
    }

    pub async fn would_block_advance(&self) -> Result<bool> {
        Ok(self.committee_next_size().await? < MIN_COMMITTEE_SIZE)
    }

    pub async fn wait_quorum(&self, min_size: usize, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let size = self.committee_size().await?;
            if size >= min_size {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for committee size >= {min_size}, got {size}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_next_quorum(&self, min_size: usize, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let size = self.committee_next_size().await?;
            if size >= min_size {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for committee_next size >= {min_size}, got {size}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Fetch on-chain state and update a single node's ChainState.
    pub async fn refresh_node_state(&self, index: usize) -> Result<()> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        trace!(index, "running manual refresh_node_state");

        let ctx = node.context();
        let state = fetch_state(&ctx.rpc)
            .await
            .map_err(|e| anyhow::anyhow!("fetch protocol state: {e}"))?;
        ctx.set_state(state)
            .map_err(|e| anyhow::anyhow!("publish protocol state: {e}"))?;
        ctx.refresh_peers()
            .await
            .map_err(|e| anyhow::anyhow!("resolve peers: {e}"))?;
        trace!(index, "manual refresh_node_state complete");
        Ok(())
    }

    /// Fetch on-chain state and update all nodes' ChainState.
    pub async fn refresh_all_nodes(&self) -> Result<()> {
        for i in 0..self.harness.nodes().len() {
            self.refresh_node_state(i)
                .await
                .with_context(|| format!("refresh node {i}"))?;
        }
        Ok(())
    }

    async fn committee_len(&self, epoch: EpochNumber) -> Result<usize> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        match client.get_committee(epoch).await {
            Ok(members) => Ok(members.len()),
            Err(rpc::RpcError::AccountNotFound(_)) => Ok(0),
            Err(error) => Err(error).context("read committee"),
        }
    }
}
