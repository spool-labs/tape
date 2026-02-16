use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tape_api::fsm::NodeAction;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

use crate::harness::fixture::network::SimNet;
use crate::harness::log::{log_path, read_log};

impl SimNet {
    pub async fn is_node_healthy(&self, index: usize) -> bool {
        let client = self.build_client(index, index);
        client.health_check().await.unwrap_or(false)
    }

    pub async fn wait_node_healthy(&self, index: usize, timeout: Duration) -> Result<()> {
        let start_time = Instant::now();
        while start_time.elapsed() < timeout {
            if self.is_node_healthy(index).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        bail!("node {} did not become healthy within {:?}", index, timeout)
    }

    pub async fn wait_nodes_healthy(&self, timeout: Duration) -> Result<()> {
        for index in 0..self.nodes.len() {
            self.wait_node_healthy(index, timeout).await?;
        }
        Ok(())
    }

    pub fn read_node_log(&self, index: usize) -> Option<String> {
        if index >= self.nodes.len() {
            return None;
        }

        let node_name = &self.nodes[index].ctx.config.name;
        let raw = read_log()?;
        let lines: Vec<&str> = raw.lines().filter(|line| line.contains(node_name)).collect();

        if lines.is_empty() {
            Some(raw)
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn read_runtime_log(&self, index: usize) -> Option<String> {
        if index >= self.nodes.len() {
            return None;
        }

        let node_id = self.nodes[index].ctx.control_plane.our_node_id().as_u64();
        let raw = read_log()?;
        let lines: Vec<&str> = raw
            .lines()
            .filter(|line| line.contains("tape_node::runtime"))
            .filter(|line| line.contains(&format!("node={node_id}")))
            .collect();

        if lines.is_empty() {
            let runtime_only: Vec<&str> = raw
                .lines()
                .filter(|line| line.contains("tape_node::runtime"))
                .collect();
            if runtime_only.is_empty() {
                None
            } else {
                Some(runtime_only.join("\n"))
            }
        } else {
            Some(lines.join("\n"))
        }
    }

    pub fn check_node_logs(&self) -> Result<()> {
        let blocked = self.blocked_count();
        if blocked > 0 {
            bail!("found {blocked} blocked node actions");
        }

        for index in 0..self.nodes.len() {
            let status = self.nodes[index]
                .ctx
                .storage
                .store
                .get_node_status()
                .ok()
                .flatten()
                .unwrap_or(NodeStatus::Standby);
            if matches!(status, NodeStatus::RecoveryReplay | NodeStatus::RecoverMetadata) {
                bail!("node {index} stayed in recovery status: {status:?}");
            }
        }

        Ok(())
    }

    pub fn node_urls(&self) -> Vec<String> {
        self.nodes
            .iter()
            .map(|node| {
                format!(
                    "https://{}:{}",
                    node.ctx.config.public_host,
                    node.ctx.config.public_port
                )
            })
            .collect()
    }

    pub fn log_file(&self) -> Option<String> {
        let _ = self.nodes.first()?;
        log_path().map(|path| path.display().to_string())
    }

    pub fn node_action(&self, index: usize) -> NodeAction {
        let now = self.nodes[index].ctx.now();
        let (action, _) = self.nodes[index].ctx.control_plane.determine_action(now);
        action
    }

    pub fn blocked_count(&self) -> usize {
        let mut count = 0usize;
        for index in 0..self.nodes.len() {
            if self.node_action(index).is_blocked() {
                count += 1;
            }
        }
        count
    }

    pub fn assert_action(&self, index: usize, expected: NodeAction) -> Result<()> {
        let got = self.node_action(index);
        if got != expected {
            bail!("node {index} action mismatch: expected {expected:?}, got {got:?}");
        }
        Ok(())
    }

    pub async fn wait_action(&self, index: usize, expected: NodeAction, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let got = self.node_action(index);
            if got == expected {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting node {index} action {expected:?}, got {got:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_blocked(&self, max_blocked: usize, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let blocked = self.blocked_count();
            if blocked <= max_blocked {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting blocked <= {max_blocked}, got {blocked}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_runtime_log(&self, index: usize, pattern: &str, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            if let Some(log) = self.read_runtime_log(index) {
                if log.contains(pattern) {
                    return Ok(());
                }
            }
            if start.elapsed() >= timeout {
                bail!("timed out waiting runtime log for node {index}: {pattern}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
