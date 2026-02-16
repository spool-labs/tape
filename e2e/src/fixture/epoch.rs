use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use tape_api::errors::TapeError;
use tape_api::instruction::build_advance_epoch_ix;
use tape_api::program::EPOCH_DURATION;
use tape_api::prelude::{Epoch, System};
use tape_core::types::EpochNumber;
use tokio::time::sleep;

use crate::harness::fixture::network::SimNet;
use crate::harness::log::append_log;

impl SimNet {
    const ADV_CU: u32 = 1_400_000;

    pub async fn current_epoch(&self) -> Result<u64> {
        let epoch = self
            .client
            .get_epoch()
            .await
            .context("read epoch from chain")?;
        Ok(epoch.id.as_u64())
    }

    pub async fn current_phase(&self) -> Result<String> {
        let epoch = self
            .client
            .get_epoch()
            .await
            .context("read epoch for phase")?;

        let phase = if epoch.state.is_syncing() {
            "Syncing"
        } else if epoch.state.is_settling() {
            "Settling"
        } else if epoch.state.is_active() {
            "Active"
        } else {
            "Unknown"
        };

        Ok(phase.to_string())
    }

    pub async fn advance_epoch(&self) -> Result<()> {
        append_log("advance epoch start");
        let mut last_error = None;

        for (index, node) in self.nodes.iter().enumerate() {
            let authority = node.ctx.pubkey();
            let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(Self::ADV_CU);
            let instruction = build_advance_epoch_ix(authority, authority);
            match self
                .client
                .send_instructions(node.ctx.keypair.as_ref(), vec![cu_ix, instruction])
                .await
            {
                Ok(_) => {
                    append_log(&format!("advance epoch done by={index}"));
                    append_log("advance epoch done");
                    return Ok(());
                }
                Err(err) => {
                    let err_text = err.to_string();
                    if let Some(tape_err) = TapeError::from_error_string(&err_text) {
                        append_log(&format!("advance epoch node={index} err={tape_err}"));
                    } else {
                        append_log(&format!("advance epoch node={index} err={err_text}"));
                    }
                    last_error = Some(err_text);
                }
            }
        }

        append_log("advance epoch fail");
        Err(anyhow!(
            "advance epoch failed for all nodes: {}",
            last_error.unwrap_or_else(|| "unknown error".to_string())
        ))
    }

    pub async fn wait_epoch(&self, target_epoch: u64, timeout: Duration) -> Result<()> {
        let start_time = Instant::now();
        loop {
            let current_epoch = self.current_epoch().await?;
            if current_epoch >= target_epoch {
                return Ok(());
            }

            if start_time.elapsed() >= timeout {
                bail!(
                    "timed out waiting for epoch {target_epoch}, current epoch is {current_epoch}"
                );
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_phase(&self, target_phase: &str, timeout: Duration) -> Result<()> {
        let start_time = Instant::now();
        loop {
            let current_phase = self.current_phase().await?;
            if current_phase == target_phase {
                return Ok(());
            }

            if start_time.elapsed() >= timeout {
                bail!(
                    "timed out waiting for phase {target_phase}, current phase is {current_phase}"
                );
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_active_epoch(&self, target_epoch: u64, timeout: Duration) -> Result<()> {
        self.wait_epoch(target_epoch, timeout).await?;
        self.wait_phase("Active", timeout).await
    }

    pub async fn observe_epochs<F>(&self, count: u64, mut check: F) -> Result<()>
    where
        F: FnMut(u64, &System) -> Result<()>,
    {
        let mut last_epoch = self.current_epoch().await?;
        let mut observed = 0u64;

        while observed < count {
            self.wait_epoch(last_epoch + 1, Duration::from_secs(30)).await?;
            let epoch = self.current_epoch().await?;
            let system = self.client.get_system().await.context("read system during observe")?;

            observed += epoch - last_epoch;
            last_epoch = epoch;
            check(epoch, &system)?;
        }

        Ok(())
    }

    pub async fn watch_epochs<F>(&self, count: u64, mut check: F) -> Result<()>
    where
        F: FnMut(&Epoch, &System) -> Result<()>,
    {
        let mut last_id = self.current_epoch().await?;
        let mut seen = 0u64;

        while seen < count {
            self.wait_epoch(last_id + 1, Duration::from_secs(30)).await?;

            let epoch = self.client.get_epoch().await.context("read epoch in watch")?;
            let system = self.client.get_system().await.context("read system in watch")?;
            let now_id = epoch.id.as_u64();

            seen += now_id.saturating_sub(last_id);
            last_id = now_id;
            check(&epoch, &system)?;
        }

        Ok(())
    }

    pub async fn wait_for_epoch(&self, target_epoch: EpochNumber, timeout: Duration) -> Result<()> {
        self.wait_active_epoch(target_epoch.as_u64(), timeout).await
    }

    pub async fn advance_slots(&self, slot_count: u64) -> Result<()> {
        if slot_count == 0 {
            return Ok(());
        }

        let base_slot = self.client.get_slot().await.context("read current slot")?;
        let target_slot = base_slot + slot_count;
        self.rpc.warp_to_slot(target_slot).context("warp litesvm slot")?;
        Ok(())
    }

    pub fn warp(&self, seconds: i64) -> Result<()> {
        append_log(&format!("warp start seconds={seconds}"));
        self.rpc
            .advance_time(seconds)
            .context("advance litesvm clock")?;
        append_log("warp done");
        Ok(())
    }

    pub fn warp_epoch(&self) -> Result<()> {
        self.warp(EPOCH_DURATION + 1)
    }

    pub async fn wait_epoch_change(&self, prev_epoch: u64, timeout: Duration) -> Result<u64> {
        let start = Instant::now();
        loop {
            let now = self.current_epoch().await?;
            if now > prev_epoch {
                return Ok(now);
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting for epoch change from {prev_epoch}, current {now}");
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
}
