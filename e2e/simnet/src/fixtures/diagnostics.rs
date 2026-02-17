use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use reqwest::Client;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

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
        while start.elapsed() < timeout {
            if self.is_node_healthy(index).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!("node {index} did not become healthy within {timeout:?}");
    }

    pub async fn wait_nodes_healthy(&self, timeout: Duration) -> Result<()> {
        for i in 0..self.harness.nodes().len() {
            self.wait_node_healthy(i, timeout).await?;
        }
        Ok(())
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
            let status = node
                .context()
                .store
                .get_node_status()
                .with_context(|| format!("read node {} status", node.id()))?
                .unwrap_or(NodeStatus::Standby);

            if matches!(status, NodeStatus::RecoverMetadata | NodeStatus::RecoveryReplay) {
                bail!("node {} remained in recovery status: {status:?}", node.id());
            }
        }
        Ok(())
    }
}
