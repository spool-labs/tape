use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tape_api::prelude::{Archive, Epoch, SnapshotState, System};
use tape_node::supervisor::{TaskKey, TaskOutcome};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    pub async fn read_system(&self) -> Result<System> {
        let client = rpc_client::RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_system().await.context("read system")
    }

    pub async fn read_epoch(&self) -> Result<Epoch> {
        let client = rpc_client::RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_epoch().await.context("read epoch")
    }

    pub async fn read_archive(&self) -> Result<Archive> {
        let client = rpc_client::RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_archive().await.context("read archive")
    }

    pub async fn read_snapshot_state(&self) -> Result<SnapshotState> {
        let client = rpc_client::RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client
            .get_snapshot_state()
            .await
            .context("read snapshot state")
    }

    pub async fn committee_size(&self) -> Result<usize> {
        Ok(self.read_system().await?.committee.size())
    }

    pub async fn committee_next_size(&self) -> Result<usize> {
        Ok(self.read_system().await?.committee_next.size())
    }

    pub async fn is_bootstrap_mode(&self) -> Result<bool> {
        Ok(self.read_system().await?.committee_prev_empty())
    }

    pub async fn is_low_quorum(&self) -> Result<bool> {
        Ok(self.read_system().await?.is_low_quorum())
    }

    pub async fn would_block_advance(&self) -> Result<bool> {
        Ok(self.read_system().await?.will_be_low_quorum())
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

    /// Run node `RefreshOnchainState` task once for a single node.
pub async fn refresh_node_state(&self, index: usize) -> Result<()> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        let semaphore = Arc::new(Semaphore::new(1));
        let (_peer_service, peer_handle) = tape_node::runtime::PeerService::new();
        let cancel = CancellationToken::new();

        let (_key, outcome) = tape_node::tasks::execute_task(
            node.context(),
            peer_handle,
            TaskKey::RefreshOnchainState,
            cancel,
            semaphore,
        )
        .await;

        match outcome {
            TaskOutcome::Success => Ok(()),
            TaskOutcome::Pending(_) => Ok(()),
            TaskOutcome::Retryable(reason) => {
                bail!("refresh_node_state({index}) retryable failure: {reason}")
            }
            TaskOutcome::Permanent(reason) => {
                bail!("refresh_node_state({index}) permanent failure: {reason}")
            }
        }
    }

    /// Run `RefreshOnchainState` for all nodes.
    pub async fn refresh_all_nodes(&self) -> Result<()> {
        for i in 0..self.harness.nodes().len() {
            self.refresh_node_state(i)
                .await
                .with_context(|| format!("refresh node {i}"))?;
        }
        Ok(())
    }
}
