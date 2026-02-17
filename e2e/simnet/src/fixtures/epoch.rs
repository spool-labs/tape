use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tape_api::instruction::build_advance_epoch_ix;
use tape_api::program::EPOCH_DURATION;
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

    /// Attempt advance-epoch with each node authority until one succeeds.
    pub async fn advance_epoch_any(&self) -> Result<()> {
        let mut last_error = None;
        for node in self.harness.nodes() {
            let ix = build_advance_epoch_ix(node.authority(), node.authority());
            match self
                .harness
                .chain()
                .send_instructions_and_advance(
                    node.keypair(),
                    vec![ix],
                    self.harness.config().slot_advance_per_tx,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => last_error = Some(e),
            }
        }

        match last_error {
            Some(e) => Err(e).context("advance_epoch_any failed for all nodes"),
            None => bail!("advance_epoch_any failed: no nodes available"),
        }
    }

    pub async fn wait_epoch(&self, target_epoch: u64, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let epoch = self.current_epoch_number().await?;
            if epoch >= target_epoch {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for epoch {target_epoch}, current {epoch}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_phase(&self, target_phase: &str, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let phase = self.current_epoch_phase().await?;
            if phase == target_phase {
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
        let start = Instant::now();
        loop {
            let now = self.current_epoch_number().await?;
            if now > previous {
                return Ok(now);
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for epoch change from {previous}, current {now}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Deterministic slot driver for explicit test control.
    pub async fn drive_slots(&self, slot_count: u64) -> Result<u64> {
        self.harness
            .chain()
            .advance_slots(slot_count)
            .await
            .context("drive_slots")
    }

    /// Deterministic slot bump policy derived from configured `slot_advance_per_tx`.
    pub async fn bump_slots_after_txs(&self, tx_count: u64) -> Result<u64> {
        let slot_count = tx_count.saturating_mul(self.harness.config().slot_advance_per_tx);
        self.drive_slots(slot_count).await
    }

    pub fn warp_seconds(&self, seconds: i64) -> Result<()> {
        self.harness
            .chain()
            .advance_time_seconds(seconds)
            .context("warp_seconds")
    }

    pub fn warp_epoch(&self) -> Result<()> {
        self.warp_seconds(EPOCH_DURATION + 1)
    }
}
