//! Test node management for e2e testing.
//!
//! Provides utilities for creating, configuring, registering, and running
//! storage nodes in test scenarios.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use tempfile::TempDir;

use tape_core::bls::BlsPrivateKey;
use tape_crypto::bls12254::min_sig::PrivKey;

use crate::Tapedrive;

/// A test node with all its configuration and state.
///
/// Manages the full lifecycle: keypair generation, config creation,
/// on-chain registration, and process management.
pub struct TestNode {
    /// Node index (for multi-node tests).
    pub index: usize,
    /// Node name.
    pub name: String,
    /// HTTP port.
    pub port: u16,
    /// Base directory for this node's data.
    pub base_dir: PathBuf,
    /// Path to node config file.
    pub config_path: PathBuf,
    /// Node authority keypair.
    pub authority: Keypair,
    /// Node PDA address (derived from authority).
    pub node_address: Option<Pubkey>,
    /// Running process handle.
    process: Option<Child>,
    /// Whether we own the base directory (for cleanup).
    owns_dir: bool,
    /// Temporary directory handle (if using temp dir).
    _temp_dir: Option<TempDir>,
}

impl TestNode {
    /// Create a new test node with generated keypairs.
    ///
    /// # Arguments
    ///
    /// * `index` - Node index (used for naming and port allocation)
    /// * `base_port` - Base port; actual port will be base_port + index
    pub fn new(index: usize, base_port: u16) -> Result<Self> {
        let temp_dir = TempDir::new().context("Failed to create temp dir for node")?;
        let base_dir = temp_dir.path().to_path_buf();

        Self::new_in_dir(index, base_port, base_dir, Some(temp_dir))
    }

    /// Create a new test node in a specific directory.
    pub fn new_in_dir(
        index: usize,
        base_port: u16,
        base_dir: PathBuf,
        temp_dir: Option<TempDir>,
    ) -> Result<Self> {
        let port = base_port + index as u16;
        let name = format!("test-node-{}", index);

        // Create directories
        let keys_dir = base_dir.join("keys");
        let data_dir = base_dir.join("data");
        std::fs::create_dir_all(&keys_dir).context("Failed to create keys dir")?;
        std::fs::create_dir_all(&data_dir).context("Failed to create data dir")?;

        // Generate keypairs
        let authority = generate_keypair(&keys_dir.join("node.json"))?;
        generate_keypair(&keys_dir.join("tls.json"))?;
        generate_bls_keypair(&keys_dir.join("bls.json"))?;

        // Write config
        let config_path = base_dir.join("node.yaml");
        let config_content = generate_node_config(&name, &base_dir, port);
        std::fs::write(&config_path, &config_content).context("Failed to write node config")?;

        Ok(Self {
            index,
            name,
            port,
            base_dir,
            config_path,
            authority,
            node_address: None,
            process: None,
            owns_dir: temp_dir.is_some(),
            _temp_dir: temp_dir,
        })
    }

    /// Create multiple test nodes.
    pub fn create_many(count: usize, base_port: u16) -> Result<Vec<Self>> {
        (0..count)
            .map(|i| Self::new(i, base_port))
            .collect()
    }

    /// Create multiple test nodes in a shared base directory.
    pub fn create_many_in_dir(
        count: usize,
        base_port: u16,
        base_dir: &Path,
    ) -> Result<Vec<Self>> {
        std::fs::create_dir_all(base_dir)?;

        (0..count)
            .map(|i| {
                let node_dir = base_dir.join(format!("node-{}", i));
                std::fs::create_dir_all(&node_dir)?;
                Self::new_in_dir(i, base_port, node_dir, None)
            })
            .collect()
    }

    /// Get the node's HTTP URL.
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Get the authority pubkey.
    pub fn authority_pubkey(&self) -> Pubkey {
        self.authority.pubkey()
    }

    /// Register this node on-chain.
    ///
    /// This calls `tape node register` with this node's config.
    /// The CLI's fee payer pays for the transaction; the node authority
    /// is loaded from the node's config file (node_keypair field).
    pub fn register(&mut self, cli: &Tapedrive) -> Result<Pubkey> {
        // Use the original CLI - fee payer from CLI, authority from node config
        let address = cli.node_register(Some(&self.config_path))?;
        self.node_address = Some(address);
        Ok(address)
    }

    /// Stake tokens to this node.
    ///
    /// Uses the provided CLI's keypair as the staker.
    pub fn stake(&self, cli: &Tapedrive, amount: u64) -> Result<Pubkey> {
        let node_addr = self.node_address.ok_or_else(|| {
            anyhow::anyhow!("Node not registered yet")
        })?;
        cli.stake_deposit(&node_addr, amount)
    }

    /// Join the committee.
    pub fn join(&self, cli: &Tapedrive) -> Result<()> {
        cli.node_join(Some(&self.config_path))
    }

    /// Advance pool accounting.
    pub fn advance_pool(&self, cli: &Tapedrive) -> Result<()> {
        cli.node_advance(Some(&self.config_path))
    }

    /// Submit epoch sync attestation.
    pub fn sync(&self, cli: &Tapedrive) -> Result<()> {
        cli.node_sync(Some(&self.config_path))
    }

    /// Start the node process.
    ///
    /// Returns a handle that can be used to check status and stop the node.
    pub fn start(&mut self, cli: &Tapedrive) -> Result<()> {
        if self.process.is_some() {
            bail!("Node already running");
        }

        let child = cli.node_start_detached(Some(&self.config_path))?;

        self.process = Some(child);
        Ok(())
    }

    /// Start the node with output to log file.
    pub fn start_with_logging(&mut self, tape_bin: &Path) -> Result<()> {
        if self.process.is_some() {
            bail!("Node already running");
        }

        let log_path = self.base_dir.join("node.log");
        let log_file = std::fs::File::create(&log_path)
            .context("Failed to create node log file")?;

        let child = Command::new(tape_bin)
            .args(["-u", "l"])
            .args(["-k", self.base_dir.join("keys/node.json").to_str().unwrap_or("")])
            .args(["node", "start"])
            .args(["--config", self.config_path.to_str().unwrap_or("")])
            .stdout(Stdio::from(log_file.try_clone()?))
            .stderr(Stdio::from(log_file))
            .spawn()
            .context("Failed to spawn node process")?;

        self.process = Some(child);
        Ok(())
    }

    /// Check if the node process is running.
    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut process) = self.process {
            match process.try_wait() {
                Ok(None) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Check if the node is healthy via HTTP.
    pub async fn is_healthy(&self) -> bool {
        let url = format!("{}/v1/health", self.url());
        let client = reqwest::Client::new();

        matches!(
            client
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await,
            Ok(resp) if resp.status().is_success()
        )
    }

    /// Wait for the node to become healthy.
    pub async fn wait_healthy(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if self.is_healthy().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        bail!("Node {} did not become healthy within {:?}", self.name, timeout)
    }

    /// Stop the node process.
    pub fn stop(&mut self) {
        if let Some(mut process) = self.process.take() {
            #[cfg(unix)]
            {
                unsafe {
                    libc::kill(process.id() as i32, libc::SIGTERM);
                }
                std::thread::sleep(Duration::from_millis(500));
            }

            let _ = process.kill();
            let _ = process.wait();
        }
    }

    /// Get the log file path.
    pub fn log_path(&self) -> PathBuf {
        self.base_dir.join("node.log")
    }

    /// Read the node's log file.
    pub fn read_log(&self) -> Result<String> {
        std::fs::read_to_string(self.log_path())
            .context("Failed to read node log")
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Generate node config YAML content.
fn generate_node_config(name: &str, node_dir: &Path, port: u16) -> String {
    let keys_dir = node_dir.join("keys");
    let data_dir = node_dir.join("data");

    format!(
        r#"# Tapedrive Storage Node Configuration (e2e test)
version: 1

name: "{name}"
commission: 500

tls_keypair: {keys_dir}/tls.json
bls_keypair: {keys_dir}/bls.json
node_keypair: {keys_dir}/node.json

bind_address: "0.0.0.0:{port}"
public_host: "127.0.0.1"
public_port: {port}

tls:
  generate_self_signed: true

storage_path: {data_dir}
"#,
        name = name,
        keys_dir = keys_dir.display(),
        data_dir = data_dir.display(),
        port = port,
    )
}

/// Generate or load an Ed25519 keypair.
fn generate_keypair(path: &Path) -> Result<Keypair> {
    if path.exists() {
        let json = std::fs::read_to_string(path)?;
        let bytes: Vec<u8> = serde_json::from_str(&json)?;
        Keypair::from_bytes(&bytes).map_err(|e| anyhow::anyhow!("Invalid keypair: {}", e))
    } else {
        let keypair = Keypair::new();
        let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
        std::fs::write(path, &json)?;
        Ok(keypair)
    }
}

/// Generate or load a BLS keypair.
fn generate_bls_keypair(path: &Path) -> Result<BlsPrivateKey> {
    if path.exists() {
        let json = std::fs::read_to_string(path)?;
        let bytes: Vec<u8> = serde_json::from_str(&json)?;
        if bytes.len() != 32 {
            bail!("Invalid BLS key length");
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(BlsPrivateKey(PrivKey(arr)))
    } else {
        let bls_key = BlsPrivateKey::from_random();
        let bytes: &[u8] = bytemuck::bytes_of(&bls_key);
        let json = serde_json::to_string(&bytes.to_vec())?;
        std::fs::write(path, &json)?;
        Ok(bls_key)
    }
}

/// A cluster of test nodes for multi-node testing.
pub struct TestCluster {
    /// All nodes in the cluster.
    pub nodes: Vec<TestNode>,
    /// Base directory for the cluster.
    pub base_dir: PathBuf,
    /// Temporary directory handle.
    _temp_dir: Option<TempDir>,
}

impl TestCluster {
    /// Create a new test cluster with N nodes.
    pub fn new(count: usize, base_port: u16) -> Result<Self> {
        let temp_dir = TempDir::new().context("Failed to create temp dir for cluster")?;
        let base_dir = temp_dir.path().to_path_buf();

        let nodes = TestNode::create_many_in_dir(count, base_port, &base_dir)?;

        Ok(Self {
            nodes,
            base_dir,
            _temp_dir: Some(temp_dir),
        })
    }

    /// Create a cluster in a specific directory.
    pub fn new_in_dir(count: usize, base_port: u16, base_dir: PathBuf) -> Result<Self> {
        let nodes = TestNode::create_many_in_dir(count, base_port, &base_dir)?;

        Ok(Self {
            nodes,
            base_dir,
            _temp_dir: None,
        })
    }

    /// Get node URLs.
    pub fn node_urls(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.url()).collect()
    }

    /// Register all nodes on-chain.
    pub fn register_all(&mut self, cli: &Tapedrive) -> Result<Vec<Pubkey>> {
        self.nodes
            .iter_mut()
            .map(|node| node.register(cli))
            .collect()
    }

    /// Stake to all nodes.
    pub fn stake_all(&self, cli: &Tapedrive, amount: u64) -> Result<Vec<Pubkey>> {
        self.nodes
            .iter()
            .map(|node| node.stake(cli, amount))
            .collect()
    }

    /// Have all nodes join the committee.
    pub fn join_all(&self, cli: &Tapedrive) -> Result<()> {
        for node in &self.nodes {
            node.join(cli)?;
        }
        Ok(())
    }

    /// Start all nodes.
    pub fn start_all(&mut self, cli: &Tapedrive) -> Result<()> {
        for node in &mut self.nodes {
            node.start(cli)?;
        }
        Ok(())
    }

    /// Wait for all nodes to be healthy.
    pub async fn wait_all_healthy(&self, timeout: Duration) -> Result<()> {
        for node in &self.nodes {
            node.wait_healthy(timeout).await?;
        }
        Ok(())
    }

    /// Stop all nodes.
    pub fn stop_all(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }

    /// Kill any existing node processes.
    pub fn kill_existing() {
        #[cfg(unix)]
        {
            let _ = Command::new("pkill")
                .args(["-f", "tape.*node.*start"])
                .output();
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self.stop_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_node_config() {
        let config = generate_node_config("test", Path::new("/tmp/test"), 8080);
        assert!(config.contains("name: \"test\""));
        assert!(config.contains("public_port: 8080"));
    }
}
