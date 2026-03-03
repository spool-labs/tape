use std::time::Duration;

use anyhow::{bail, Result};
use tracing::trace;
use tape_core::types::EpochNumber;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    pub async fn current_epoch_number(&self) -> Result<u64> {
        Ok(self.read_epoch().await?.id.as_u64())
    }

    pub async fn current_epoch_phase(&self) -> Result<&'static str> {
        let epoch = self.read_epoch().await?;
        if epoch.state.is_syncing() {
            Ok("Syncing")
        } else if epoch.state.is_settling() {
            Ok("Settling")
        } else if epoch.state.is_active() {
            Ok("Active")
        } else {
            Ok("Unknown")
        }
    }

    pub async fn wait_epoch(&self, target_epoch: u64, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        trace!(target_epoch, timeout_secs = timeout.as_secs(), "waiting for target epoch");
        loop {
            let epoch = self.current_epoch_number().await?;
            if epoch >= target_epoch {
                trace!(target_epoch, observed_epoch = epoch, "epoch target reached");
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for epoch {target_epoch}, current {epoch}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_phase(&self, target_phase: &str, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        trace!(target_phase, timeout_secs = timeout.as_secs(), "waiting for target phase");
        loop {
            let phase = self.current_epoch_phase().await?;
            if phase == target_phase {
                trace!(phase = target_phase, "phase target reached");
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for phase {target_phase}, current {phase}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_active_epoch(&self, target_epoch: EpochNumber, timeout: Duration) -> Result<()> {
        self.wait_epoch(target_epoch.as_u64(), timeout).await?;
        self.wait_phase("Active", timeout).await
    }

    pub async fn wait_epoch_change(&self, previous: u64, timeout: Duration) -> Result<u64> {
        let start = std::time::Instant::now();
        trace!(previous, timeout_secs = timeout.as_secs(), "waiting for epoch change");
        loop {
            let now = self.current_epoch_number().await?;
            if now > previous {
                trace!(previous, next = now, "epoch change observed");
                return Ok(now);
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for epoch change from {previous}, current {now}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait for the nodes to self-advance the epoch.
    pub async fn self_advance_epoch(&self, timeout: Duration) -> Result<u64> {
        let current = self.current_epoch_number().await?;
        let next = self.wait_epoch_change(current, timeout).await?;
        self.wait_phase("Active", timeout).await?;
        Ok(next)
    }

    /// Drive epoch transitions with a participating node subset.
    pub async fn advance_to_epoch(
        &self,
        target_epoch: u64,
        participants: &[usize],
        timeout: Duration,
    ) -> Result<()> {
        if participants.is_empty() {
            bail!("advance_to_epoch requires non-empty participants");
        }

        loop {
            let current = self.current_epoch_number().await?;
            if current >= target_epoch {
                return Ok(());
            }
            trace!(
                current,
                target = target_epoch,
                participants = participants.len(),
                "advance_to_epoch step begin"
            );

            self.wait_phase("Active", timeout).await?;
            self.pool_many(participants).await?;
            self.join_many(participants).await?;
            self.wait_next_quorum(participants.len(), timeout).await?;
            self.wait_snapshot_ready_for(current, timeout).await?;

            let next = self.wait_epoch_change(current, timeout).await?;
            self.wait_phase("Active", timeout).await?;
            self.wait_for_nodes_epoch(participants, Some(EpochNumber(next)), timeout)
                .await?;
        }
    }

    async fn wait_snapshot_ready_for(&self, current_epoch: u64, timeout: Duration) -> Result<()> {
        if current_epoch <= 1 {
            return Ok(());
        }
        let required = current_epoch.saturating_sub(1);
        let start = std::time::Instant::now();
        loop {
            let state = self.read_snapshot_state().await?;
            let latest = state.latest_epoch.as_u64();
            if latest >= required {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!(
                    "timed out waiting snapshot latest_epoch >= {required}; latest={}, certifying={}, certified_count={}",
                    latest,
                    state.certifying_epoch.as_u64(),
                    state.certified_count
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
