use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tape_api::errors::ProgramError;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use tape_api::instruction::build_advance_epoch_ix;
use tape_api::program::EPOCH_DURATION;
use tape_core::types::EpochNumber;

use crate::log::append_log;
use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    const ADV_CU: u32 = 1_400_000;

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
            let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::ADV_CU);
            let ix = build_advance_epoch_ix(node.authority(), node.authority());
            match self
                .harness
                .chain()
                .send_instructions_and_advance(
                    node.keypair(),
                    vec![cu_ix, ix],
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

    /// Drive epoch transitions with a participating node subset.
    pub async fn advance_to_epoch(
        &self,
        target_epoch: u64,
        payer_index: usize,
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

            self.wait_phase("Active", timeout).await?;
            self.pool_many(payer_index, participants).await?;
            self.join_many(payer_index, participants).await?;
            self.wait_next_quorum(participants.len(), timeout).await?;
            self.wait_snapshot_ready_for(current, timeout).await?;

            self.warp_epoch()?;
            self.advance_epoch_with_retry(timeout).await?;
            let next = self.wait_epoch_change(current, timeout).await?;
            self.wait_phase("Active", timeout).await?;
            self.wait_for_nodes_epoch(participants, Some(EpochNumber(next)), timeout)
                .await?;
        }
    }

    async fn advance_epoch_with_retry(&self, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            match self.advance_epoch_any().await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    let diag = self.snapshot_diag().await;
                    append_log(&format!("advance_epoch retryable={:?} snapshot={diag}", error));
                    if start.elapsed() >= timeout || !is_retryable_advance_error(&error) {
                        return Err(error)
                            .context(format!("advance_epoch_with_retry snapshot={diag}"));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_snapshot_ready_for(&self, current_epoch: u64, timeout: Duration) -> Result<()> {
        if current_epoch <= 1 {
            return Ok(());
        }
        let required = current_epoch.saturating_sub(1);
        let start = Instant::now();
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

    async fn snapshot_diag(&self) -> String {
        match self.read_snapshot_state().await {
            Ok(state) => format!(
                "latest={} certifying={} certified_count={}",
                state.latest_epoch.as_u64(),
                state.certifying_epoch.as_u64(),
                state.certified_count
            ),
            Err(e) => format!("snapshot_state_error={e:#}"),
        }
    }
}

fn is_retryable_advance_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}");
    ProgramError::from_error_string(&text)
        .map(|err| err.is_retriable())
        .unwrap_or(false)
}
