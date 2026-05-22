use std::time::Duration;

use anyhow::{bail, Result};
use tape_core::system::EpochPhase;
use tracing::trace;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    pub async fn current_epoch_number(&self) -> Result<u64> {
        Ok(self.read_epoch().await?.id.as_u64())
    }

    pub async fn current_epoch_phase(&self) -> Result<&'static str> {
        let epoch = self.read_epoch().await?;
        match epoch.state.phase() {
            Some(EpochPhase::Sync) => Ok("Sync"),
            Some(EpochPhase::Snapshot) => Ok("Snapshot"),
            Some(EpochPhase::Active) => Ok("Active"),
            Some(EpochPhase::Closing) => Ok("Closing"),
            Some(EpochPhase::Completed) => Ok("Completed"),
            Some(EpochPhase::Unknown) | None => Ok("Unknown"),
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
}
