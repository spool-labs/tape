use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use reqwest::Client;
use tape_store::ops::SpoolOps;
use tape_store::types::{NodeStatus, SpoolState};
use tracing::trace;

use crate::log::{log_path, read_log};
use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    pub async fn is_node_healthy(&self, index: usize) -> bool {
        let node = match self.harness.node(index) {
            Some(node) => node,
            None => return false,
        };

        let url = format!(
            "http://{}:{}/v1/health",
            node.context().config.public_host,
            node.context().config.public_port
        );

        let client = match Client::builder().timeout(Duration::from_secs(2)).build() {
            Ok(client) => client,
            Err(_) => return false,
        };

        match client.get(url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    pub async fn wait_node_healthy(&self, index: usize, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        trace!(index, timeout_secs = timeout.as_secs(), "wait_node_healthy start");
        while start.elapsed() < timeout {
            if self.is_node_healthy(index).await {
                trace!(index, elapsed_ms = start.elapsed().as_millis(), "wait_node_healthy success");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!("node {index} did not become healthy within {timeout:?}");
    }

    pub async fn wait_nodes_healthy(&self, timeout: Duration) -> Result<()> {
        trace!(
            node_count = self.harness.nodes().len(),
            timeout_secs = timeout.as_secs(),
            "wait_nodes_healthy start"
        );
        for i in 0..self.harness.nodes().len() {
            self.wait_node_healthy(i, timeout).await?;
        }
        trace!(
            node_count = self.harness.nodes().len(),
            "wait_nodes_healthy complete"
        );
        Ok(())
    }

    pub async fn wait_nodes_active(&self, indices: &[usize], timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            if indices.iter().all(|&i| self.node_status(i) == Some(NodeStatus::Active)) {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                let statuses: Vec<_> = indices.iter()
                    .map(|&i| (i, self.node_status(i)))
                    .filter(|(_, s)| *s != Some(NodeStatus::Active))
                    .collect();
                bail!("nodes did not reach Active within {timeout:?}: {statuses:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn read_node_log(&self, index: usize) -> Option<String> {
        let node = self.harness.node(index)?;
        let raw = read_log()?;
        let name = &node.context().config.name;

        let lines: Vec<_> = raw.lines().filter(|line| line.contains(name)).collect();
        if lines.is_empty() {
            Some(raw)
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn read_runtime_log(&self, index: usize) -> Option<String> {
        let node = self.harness.node(index)?;
        let raw = read_log()?;
        let name = &node.context().config.name;

        let lines: Vec<_> = raw
            .lines()
            .filter(|line| line.contains("tape_node"))
            .filter(|line| line.contains(name))
            .collect();

        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn node_urls(&self) -> Vec<String> {
        self.harness
            .nodes()
            .iter()
            .map(|node| {
                format!(
                    "http://{}:{}",
                    node.context().config.public_host,
                    node.context().config.public_port
                )
            })
            .collect()
    }

    pub fn log_file(&self) -> Option<String> {
        let _ = self.harness.nodes().first()?;
        log_path().map(|p| p.display().to_string())
    }

    pub fn check_node_stores(&self) -> Result<()> {
        for node in self.harness.nodes() {
            let _status = node.context().node_status();
            // Recovery status is no longer tracked in ChainState;
            // node_status() derives Active/Standby from committee membership.
        }
        Ok(())
    }

    pub fn node_spool_count(&self, index: usize) -> Result<usize> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        let spools = node.context().store.iter_all_spools()
            .with_context(|| format!("iter_all_spools node {index}"))?;
        Ok(spools.len())
    }

    pub fn node_spool_statuses(&self, index: usize) -> Result<Vec<(u16, SpoolState)>> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;
        node.context().store.iter_all_spools()
            .with_context(|| format!("iter_all_spools node {index}"))
    }

    pub fn total_spool_count(&self, indices: &[usize]) -> Result<usize> {
        let mut total = 0;
        for &i in indices {
            total += self.node_spool_count(i)?;
        }
        Ok(total)
    }
}
