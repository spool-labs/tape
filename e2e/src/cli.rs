//! CLI wrapper for calling the `tapedrive` binary.
//!
//! Provides a type-safe interface to all CLI commands, executing them
//! as subprocesses and parsing their output.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

use tape_core::types::EpochNumber;

/// CLI wrapper for the `tapedrive` binary.
///
/// All methods execute the CLI as a subprocess and return parsed results.
#[derive(Debug, Clone)]
pub struct Tapedrive {
    /// Path to the tape binary.
    bin_path: PathBuf,
    /// Cluster flag (-u): "l" for localnet, "d" for devnet, etc.
    cluster: String,
    /// Path to the keypair file for signing transactions.
    keypair: PathBuf,
    /// Output format (always "json" for parsing).
    output_format: String,
}

impl Tapedrive {
    /// Create a new CLI wrapper with explicit configuration.
    pub fn new(bin_path: impl Into<PathBuf>, cluster: &str, keypair: impl Into<PathBuf>) -> Self {
        Self {
            bin_path: bin_path.into(),
            cluster: cluster.to_string(),
            keypair: keypair.into(),
            output_format: "json".to_string(),
        }
    }

    /// Create a CLI wrapper configured for localnet with default paths.
    ///
    /// Uses:
    /// - Binary: `target/debug/tapedrive` (relative to workspace root)
    /// - Cluster: `l` (localnet)
    /// - Keypair: `~/.config/solana/id.json`
    pub fn new_localnet() -> Self {
        let bin_path = find_workspace_root()
            .map(|root| root.join("target/debug/tapedrive"))
            .unwrap_or_else(|_| {
                std::env::current_dir()
                    .unwrap_or_default()
                    .join("target/debug/tapedrive")
            });

        let keypair = dirs::home_dir()
            .map(|h: PathBuf| h.join(".config/solana/id.json"))
            .unwrap_or_else(|| PathBuf::from("~/.config/solana/id.json"));

        Self::new(bin_path, "l", keypair)
    }

    /// Create a CLI wrapper using a specific keypair.
    pub fn with_keypair(&self, keypair: impl Into<PathBuf>) -> Self {
        let mut cli = self.clone();
        cli.keypair = keypair.into();
        cli
    }

    /// Create a CLI wrapper using a specific binary path.
    pub fn with_bin_path(&self, bin_path: impl Into<PathBuf>) -> Self {
        let mut cli = self.clone();
        cli.bin_path = bin_path.into();
        cli
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Build base command with common flags.
    fn cmd(&self) -> Command {
        let mut cmd = Command::new(&self.bin_path);
        cmd.args(["-u", &self.cluster]);
        cmd.args(["-k", self.keypair.to_str().unwrap_or("")]);
        // Note: -o json not supported by all commands, use selectively
        cmd
    }

    /// Build command with JSON output format (for commands that support it).
    fn cmd_json(&self) -> Command {
        let mut cmd = self.cmd();
        cmd.args(["-o", &self.output_format]);
        cmd
    }

    /// Execute command and check for success.
    fn exec(&self, mut cmd: Command) -> Result<Output> {
        let output = cmd.output().context("Failed to execute tapedrive CLI")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            bail!(
                "CLI command failed (exit {}): {}\n{}",
                output.status.code().unwrap_or(-1),
                stderr.trim(),
                stdout.trim()
            );
        }

        Ok(output)
    }

    /// Execute command and parse JSON output.
    fn exec_json<T: for<'de> Deserialize<'de>>(&self, cmd: Command) -> Result<T> {
        let output = self.exec(cmd)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(&stdout)
            .with_context(|| format!("Failed to parse JSON output: {}", stdout))
    }

    /// Execute command and return stdout as string.
    fn exec_stdout(&self, cmd: Command) -> Result<String> {
        let output = self.exec(cmd)?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    // =========================================================================
    // Admin commands
    // =========================================================================

    /// Initialize the system (mint, system account, epoch account).
    ///
    /// Equivalent to: `tape admin init`
    pub fn admin_init(&self) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["admin", "init"]);
        self.exec(cmd)?;
        Ok(())
    }

    /// Advance to the next epoch.
    ///
    /// Equivalent to: `tape admin advance-epoch`
    pub fn admin_advance_epoch(&self) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["admin", "advance-epoch"]);
        self.exec(cmd)?;
        Ok(())
    }

    // =========================================================================
    // Node commands
    // =========================================================================

    /// Initialize node configuration file.
    ///
    /// Equivalent to: `tape node init [--force] [--config <path>]`
    pub fn node_init(&self, config: Option<&Path>, force: bool) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "init"]);
        if force {
            cmd.arg("--force");
        }
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Register a node on-chain.
    ///
    /// Equivalent to: `tape node register [--config <path>]`
    pub fn node_register(&self, config: Option<&Path>) -> Result<Pubkey> {
        let mut cmd = self.cmd();
        cmd.args(["node", "register"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        let output = self.exec_stdout(cmd)?;

        // Try to parse pubkey from output
        // Expected format varies, try to find a pubkey in the output
        parse_pubkey_from_output(&output)
    }

    /// Request to join the committee.
    ///
    /// Equivalent to: `tape node join [--config <path>]`
    pub fn node_join(&self, config: Option<&Path>) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "join"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Start the storage node (returns immediately, node runs in background).
    ///
    /// Equivalent to: `tape node start [--config <path>]`
    ///
    /// Note: This spawns the node as a child process. Use `TestNode::start()`
    /// for managed node lifecycle.
    pub fn node_start_detached(&self, config: Option<&Path>) -> Result<std::process::Child> {
        let mut cmd = self.cmd();
        cmd.args(["node", "start"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        cmd.spawn().context("Failed to spawn node process")
    }

    /// Submit epoch sync attestation.
    ///
    /// Equivalent to: `tape node sync [--config <path>]`
    pub fn node_sync(&self, config: Option<&Path>) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "sync"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Advance pool epoch accounting.
    ///
    /// Equivalent to: `tape node advance [--config <path>]`
    pub fn node_advance(&self, config: Option<&Path>) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "advance"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Set commission rate for a node.
    ///
    /// Equivalent to: `tape node set-commission <bps> [--config <path>]`
    pub fn node_set_commission(&self, bps: u64, config: Option<&Path>) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "set-commission", &bps.to_string()]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Claim accumulated commission for a node.
    ///
    /// Equivalent to: `tape node claim-commission [--config <path>]`
    pub fn node_claim_commission(&self, config: Option<&Path>) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["node", "claim-commission"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    /// Get node status.
    ///
    /// Equivalent to: `tape node status [--config <path>] [--node <pubkey>]`
    pub fn node_status(&self, config: Option<&Path>, node: Option<&Pubkey>) -> Result<NodeStatus> {
        let mut cmd = self.cmd();
        cmd.args(["node", "status"]);
        if let Some(config) = config {
            cmd.args(["--config", config.to_str().unwrap_or("")]);
        }
        if let Some(node) = node {
            cmd.args(["--node", &node.to_string()]);
        }
        self.exec_json(cmd)
    }

    // =========================================================================
    // Stake commands
    // =========================================================================

    /// Deposit stake to a node pool.
    ///
    /// Equivalent to: `tape stake deposit <pool> <amount>`
    /// Amount is in TAPE tokens (e.g., 1000 = 1000 TAPE).
    pub fn stake_deposit(&self, pool: &Pubkey, amount_tape: u64) -> Result<Pubkey> {
        let mut cmd = self.cmd();
        cmd.args(["stake", "deposit", &pool.to_string(), &amount_tape.to_string()]);
        let output = self.exec_stdout(cmd)?;
        parse_pubkey_from_output(&output)
    }

    /// Request stake unlock.
    ///
    /// Equivalent to: `tape stake unlock <stake>`
    pub fn stake_unlock(&self, stake: &Pubkey) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["stake", "unlock", &stake.to_string()]);
        self.exec(cmd)?;
        Ok(())
    }

    /// Withdraw unlocked stake.
    ///
    /// Equivalent to: `tape stake withdraw <stake>`
    pub fn stake_withdraw(&self, stake: &Pubkey) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args(["stake", "withdraw", &stake.to_string()]);
        self.exec(cmd)?;
        Ok(())
    }

    // =========================================================================
    // Tape commands
    // =========================================================================

    /// Initialize a new tape (reserve storage capacity).
    ///
    /// Equivalent to: `tape tape init --size <mb> --start-epoch <e> --end-epoch <e>`
    pub fn tape_init(
        &self,
        size_mb: u64,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
    ) -> Result<Pubkey> {
        let mut cmd = self.cmd();
        cmd.args([
            "tape",
            "init",
            "--size",
            &size_mb.to_string(),
            "--start-epoch",
            &start_epoch.0.to_string(),
            "--end-epoch",
            &end_epoch.0.to_string(),
        ]);
        let output = self.exec_stdout(cmd)?;
        parse_pubkey_from_output(&output)
    }

    // =========================================================================
    // Storage commands
    // =========================================================================

    /// Upload a file to storage.
    ///
    /// Equivalent to: `tape storage upload <file> [--tape <tape>] [--nodes <urls>]`
    pub fn storage_upload(
        &self,
        file: &Path,
        tape: Option<&Pubkey>,
        nodes: Option<&[String]>,
    ) -> Result<StorageUploadResult> {
        let mut cmd = self.cmd();
        cmd.args(["storage", "upload", file.to_str().unwrap_or("")]);
        if let Some(tape) = tape {
            cmd.args(["--tape", &tape.to_string()]);
        }
        if let Some(nodes) = nodes {
            cmd.args(["--nodes", &nodes.join(",")]);
        }
        let output = self.exec_stdout(cmd)?;
        parse_storage_upload_output(&output)
    }

    /// Download a blob from storage.
    ///
    /// Equivalent to: `tape storage download <track_id> [-O <outfile>] [--nodes <urls>]`
    pub fn storage_download(
        &self,
        track_id: &str,
        outfile: &Path,
        nodes: Option<&[String]>,
    ) -> Result<()> {
        let mut cmd = self.cmd();
        cmd.args([
            "storage",
            "download",
            track_id,
            "-O",
            outfile.to_str().unwrap_or(""),
        ]);
        if let Some(nodes) = nodes {
            cmd.args(["--nodes", &nodes.join(",")]);
        }
        self.exec(cmd)?;
        Ok(())
    }

    // =========================================================================
    // Network commands
    // =========================================================================

    /// Ping a storage node.
    ///
    /// Equivalent to: `tape network ping <node> [-c <count>]`
    pub fn network_ping(&self, node_url: &str, count: Option<u32>) -> Result<PingResult> {
        let mut cmd = self.cmd();
        cmd.args(["network", "ping", node_url]);
        if let Some(count) = count {
            cmd.args(["-c", &count.to_string()]);
        }
        self.exec_json(cmd)
    }

    /// Check node health via HTTP.
    ///
    /// This bypasses the CLI and directly hits the node's health endpoint.
    pub async fn check_node_health(&self, url: &str) -> Result<bool> {
        let health_url = format!("{}/v1/health", url.trim_end_matches('/'));
        let client = reqwest::Client::new();

        match client
            .get(&health_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    // =========================================================================
    // Utility commands
    // =========================================================================

    /// Airdrop SOL to an address (localnet only).
    ///
    /// Uses `solana airdrop` command.
    pub fn airdrop(&self, address: &Pubkey, amount_sol: u64) -> Result<()> {
        let mut cmd = Command::new("solana");
        cmd.args(["airdrop", &amount_sol.to_string(), &address.to_string()]);
        cmd.args(["--url", "http://127.0.0.1:8899"]);

        let output = cmd.output().context("Failed to execute solana airdrop")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Airdrop might fail if already funded, which is okay
            if !stderr.contains("airdrop request") {
                bail!("Airdrop failed: {}", stderr.trim());
            }
        }

        Ok(())
    }

    /// Transfer SOL from the CLI keypair to another address.
    ///
    /// Uses `solana transfer` command.
    pub fn transfer_sol(&self, to: &Pubkey, amount_sol: f64) -> Result<()> {
        let mut cmd = Command::new("solana");
        cmd.args(["transfer", &to.to_string(), &amount_sol.to_string()]);
        cmd.args(["--url", "http://127.0.0.1:8899"]);
        cmd.args(["--keypair", self.keypair.to_str().unwrap_or("")]);
        cmd.arg("--allow-unfunded-recipient");

        let output = cmd.output().context("Failed to execute solana transfer")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Transfer failed: {}", stderr.trim());
        }

        Ok(())
    }
}

// =============================================================================
// Response types
// =============================================================================

/// Result from storage upload.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageUploadResult {
    pub track_id: String,
    pub track_address: Option<String>,
    pub tape_address: Option<String>,
    pub merkle_root: Option<String>,
    pub commitment: Option<String>,
}

/// Node status response.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub node_id: Option<u64>,
    pub authority: Option<String>,
    pub name: Option<String>,
    pub stake: Option<u64>,
    pub commission: Option<u64>,
    pub spool_count: Option<u16>,
    pub network_address: Option<String>,
}

/// Ping result.
#[derive(Debug, Clone, Deserialize)]
pub struct PingResult {
    pub success: Option<bool>,
    pub latency_ms: Option<u64>,
}

// =============================================================================
// Helper functions
// =============================================================================

/// Find the workspace root directory.
fn find_workspace_root() -> Result<PathBuf> {
    let mut current = std::env::current_dir()?;

    loop {
        if current.join("Cargo.toml").exists() {
            // Check if this is the workspace root (has [workspace] section)
            let content = std::fs::read_to_string(current.join("Cargo.toml"))?;
            if content.contains("[workspace]") {
                return Ok(current);
            }
        }

        if !current.pop() {
            bail!("Could not find workspace root");
        }
    }
}

/// Try to extract a pubkey from CLI output.
///
/// Looks for base58-encoded pubkeys in the output.
fn parse_pubkey_from_output(output: &str) -> Result<Pubkey> {
    // Try to parse as JSON first
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(output) {
        // Look for common field names
        for field in ["address", "pubkey", "node_address", "stake_address", "tape_address"] {
            if let Some(addr) = json.get(field).and_then(|v| v.as_str()) {
                if let Ok(pubkey) = addr.parse::<Pubkey>() {
                    return Ok(pubkey);
                }
            }
        }
    }

    // Fall back to scanning for base58 strings
    for word in output.split_whitespace() {
        // Solana pubkeys are 32-44 chars of base58
        let cleaned = word.trim_matches(|c: char| !c.is_alphanumeric());
        if cleaned.len() >= 32 && cleaned.len() <= 44 {
            if let Ok(pubkey) = cleaned.parse::<Pubkey>() {
                return Ok(pubkey);
            }
        }
    }

    bail!("Could not find pubkey in output: {}", output)
}

/// Parse storage upload output.
///
/// Expected format includes lines like:
/// - Key: <hex>
/// - Merkle root: <hex>
/// - Tape: <pubkey>
/// - Track: <pubkey>
fn parse_storage_upload_output(output: &str) -> Result<StorageUploadResult> {
    let mut track_address = None;
    let mut tape_address = None;
    let mut merkle_root = None;
    let mut commitment = None;

    for line in output.lines() {
        let line = line.trim();
        // Filter out ANSI escape codes
        let clean_line = strip_ansi_codes(line);
        let clean_line = clean_line.trim();

        // The Address field is the track PDA - this is what's used for slice storage/retrieval
        if clean_line.starts_with("Address:") {
            if let Some(value) = clean_line.strip_prefix("Address:") {
                track_address = Some(value.trim().to_string());
            }
        } else if clean_line.starts_with("Tape:") {
            if let Some(value) = clean_line.strip_prefix("Tape:") {
                tape_address = Some(value.trim().to_string());
            }
        } else if clean_line.starts_with("Merkle root:") || clean_line.starts_with("Merkle Root:") {
            if let Some(value) = clean_line.split(':').nth(1) {
                merkle_root = Some(value.trim().to_string());
            }
        } else if clean_line.starts_with("Commitment:") {
            if let Some(value) = clean_line.strip_prefix("Commitment:") {
                commitment = Some(value.trim().to_string());
            }
        }
    }

    let track_id = track_address.clone()
        .ok_or_else(|| anyhow::anyhow!("Could not find Address in upload output"))?;

    Ok(StorageUploadResult {
        track_id,
        track_address,
        tape_address,
        merkle_root,
        commitment,
    })
}

/// Strip ANSI escape codes from a string.
fn strip_ansi_codes(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Skip until we hit a letter (end of sequence)
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pubkey_from_output() {
        let pubkey = Pubkey::new_unique();
        let output = format!("Node registered at: {}", pubkey);
        assert_eq!(parse_pubkey_from_output(&output).unwrap(), pubkey);

        let json_output = format!(r#"{{"address": "{}"}}"#, pubkey);
        assert_eq!(parse_pubkey_from_output(&json_output).unwrap(), pubkey);
    }
}
