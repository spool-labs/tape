use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::harness::fixture::network::SimNet;
use crate::harness::log::append_log;
use crate::harness::node;

const START_TRIES: usize = 3;
const START_DELAY_MS: u64 = 200;

impl SimNet {
    pub async fn add_nodes(&mut self, count: usize) -> Result<Vec<usize>> {
        let start_index = self.next_index();
        let new_nodes = node::add_nodes(&self.rpc, std::sync::Arc::clone(&self.payer), start_index, count)
            .await
            .context("add nodes")?;

        let mut indexes = Vec::with_capacity(new_nodes.len());
        for offset in 0..new_nodes.len() {
            indexes.push(start_index + offset);
        }

        self.push_nodes(new_nodes);
        Ok(indexes)
    }

    pub async fn add_nodes_ready(&mut self, count: usize, stake_amount: u64) -> Result<Vec<usize>> {
        let indexes = self.add_nodes(count).await?;

        for &index in &indexes {
            self.stake_node(index, stake_amount)
                .await
                .with_context(|| format!("stake node {}", index))?;
            self.advance_pool_ok(index)
                .await
                .with_context(|| format!("advance pool for node {}", index))?;
            self.join_node_ok(index)
                .await
                .with_context(|| format!("join node {}", index))?;
        }

        Ok(indexes)
    }

    pub async fn start_nodes(&mut self) -> Result<()> {
        self.start_all(1, Duration::from_millis(0)).await
    }

    pub async fn start_all(&mut self, tries: usize, delay: Duration) -> Result<()> {
        let tries = tries.max(1);
        append_log(&format!("start all count={} tries={tries} delay_ms={}", self.nodes.len(), delay.as_millis()));

        for attempt in 0..tries {
            append_log(&format!("start all attempt={}", attempt + 1));
            let mut all_ok = true;
            for index in 0..self.nodes.len() {
                if !self.start_node(index).await {
                    all_ok = false;
                }
            }

            if all_ok {
                append_log("start all done");
                return Ok(());
            }

            if attempt + 1 < tries {
                tokio::time::sleep(delay).await;
            }
        }

        let mut failed = Vec::new();
        for index in 0..self.nodes.len() {
            if self
                .start_err(index)
                .map(|msg| msg.contains("api bind not permitted"))
                .unwrap_or(false)
            {
                continue;
            }
            if self.start_node(index).await {
                continue;
            }
            let reason = self.start_err(index).unwrap_or("unknown error");
            failed.push(format!("{index}: {reason}"));
        }

        if failed.is_empty() {
            append_log("start all done (api skip only)");
            return Ok(());
        }
        append_log(&format!("start all fail {}", failed.join(" | ")));
        bail!("failed to start nodes: {}", failed.join(", "))
    }

    pub async fn bootstrap_nodes(&mut self, stake_amount: u64, timeout: Duration) -> Result<()> {
        append_log(&format!("bootstrap nodes start count={} stake={stake_amount}", self.nodes.len()));
        for index in 0..self.nodes.len() {
            self.stake_node(index, stake_amount)
                .await
                .with_context(|| format!("stake node {}", index))?;
            self.advance_pool_ok(index)
                .await
                .with_context(|| format!("advance pool for node {}", index))?;
            self.join_node_ok(index)
                .await
                .with_context(|| format!("join node {}", index))?;
        }

        self.start_all(START_TRIES, Duration::from_millis(START_DELAY_MS))
            .await?;
        for index in 0..self.nodes.len() {
            self.wait_runtime_log(index, "Runtime starting", timeout)
                .await
                .with_context(|| format!("runtime did not start for node {index}"))?;
        }
        self.refresh_nodes().await;

        append_log("bootstrap nodes done");
        Ok(())
    }

    pub async fn bootstrap_to_epoch(
        &mut self,
        stake_amount: u64,
        target_epoch: u64,
        timeout: Duration,
    ) -> Result<()> {
        self.bootstrap_nodes(stake_amount, timeout).await?;
        self.wait_active_epoch(target_epoch, timeout).await
    }
}
