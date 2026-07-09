use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rpc_client::RpcClient;
use tape_api::prelude::{Archive, Epoch, Group, System};
use tape_core::spooler::GroupIndex;
use tape_core::system::Member;
use tape_core::types::EpochNumber;

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

    pub async fn read_epoch_at(&self, epoch: EpochNumber) -> Result<Epoch> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client
            .get_epoch(epoch)
            .await
            .with_context(|| format!("read epoch {}", epoch.0))
    }

    pub async fn read_archive(&self) -> Result<Archive> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client.get_archive().await.context("read archive")
    }

    pub async fn read_committee(&self, epoch: EpochNumber) -> Result<Vec<Member>> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client
            .get_committee(epoch)
            .await
            .with_context(|| format!("read committee for epoch {}", epoch.0))
    }

    pub async fn read_groups(
        &self,
        epoch: EpochNumber,
        total_groups: u64,
    ) -> Result<Vec<Group>> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client
            .get_groups(epoch, total_groups)
            .await
            .with_context(|| format!("read groups for epoch {}", epoch.0))
    }

    pub async fn read_group(
        &self,
        epoch: EpochNumber,
        group: GroupIndex,
    ) -> Result<Group> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        client
            .get_group(epoch, group)
            .await
            .with_context(|| format!("read group {} for epoch {}", group.0, epoch.0))
    }

    pub async fn committee_size(&self) -> Result<usize> {
        let system = self.read_system().await?;
        self.committee_len(system.current_epoch).await
    }

    pub async fn committee_next_size(&self) -> Result<usize> {
        let system = self.read_system().await?;
        self.committee_len(system.current_epoch.next()).await
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

    async fn committee_len(&self, epoch: EpochNumber) -> Result<usize> {
        let client = RpcClient::from_rpc(self.harness.chain().rpc().clone());
        match client.get_committee(epoch).await {
            Ok(members) => Ok(members.len()),
            Err(rpc::RpcError::AccountNotFound(_)) => Ok(0),
            Err(error) => Err(error).context("read committee"),
        }
    }
}
