//! Local Solana validator management for e2e testing.
//!
//! Handles starting and stopping the solana-test-validator with the
//! Tapedrive programs pre-deployed.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

/// Default RPC URL for localnet.
pub const LOCALNET_RPC_URL: &str = "http://127.0.0.1:8899";

/// Default WebSocket URL for localnet.
pub const LOCALNET_WS_URL: &str = "ws://127.0.0.1:8900";

/// Manages a local Solana validator process.
///
/// The validator is automatically stopped when this struct is dropped.
///
/// # Example
///
/// ```ignore
/// let validator = Validator::spawn().await?;
/// // validator is now running...
/// // automatically stopped on drop
/// ```
pub struct Validator {
    /// Child process handle.
    process: Option<Child>,
    /// Temporary ledger directory (cleaned up on drop).
    ledger_dir: Option<TempDir>,
    /// Path to custom ledger directory (if provided).
    custom_ledger_path: Option<PathBuf>,
    /// RPC URL.
    rpc_url: String,
}

impl Validator {
    /// Spawn a new validator using `make validator`.
    ///
    /// This uses the Makefile target which handles program deployment
    /// and proper configuration.
    pub async fn spawn() -> Result<Self> {
        Self::spawn_with_options(ValidatorOptions::default()).await
    }

    /// Spawn with custom options.
    ///
    /// If a validator is already running on the same port, it will be killed first.
    pub async fn spawn_with_options(options: ValidatorOptions) -> Result<Self> {
        // Kill any existing validator to ensure clean state
        if Self::is_port_in_use(options.rpc_port).await {
            Self::kill_existing();
            // Wait for port to be released
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let workspace_root = find_workspace_root()?;

        // Create temporary ledger directory if not provided
        let (ledger_dir, ledger_path) = if let Some(path) = &options.ledger_path {
            (None, path.clone())
        } else {
            let temp = TempDir::new().context("Failed to create temp ledger dir")?;
            let path = temp.path().to_path_buf();
            (Some(temp), path)
        };

        // Build the validator command
        let mut cmd = if options.use_makefile {
            // Use make validator (recommended - handles all setup)
            let mut cmd = Command::new("make");
            cmd.arg("validator");
            cmd.current_dir(&workspace_root);
            cmd
        } else {
            // Direct solana-test-validator invocation
            let mut cmd = Command::new("solana-test-validator");
            cmd.args(["--ledger", ledger_path.to_str().unwrap_or("/tmp/ledger")]);
            cmd.args(["--rpc-port", &options.rpc_port.to_string()]);
            cmd.arg("--reset");

            // Add program deployments if specified
            for (program_id, so_path) in &options.programs {
                cmd.args(["--bpf-program", program_id, so_path.to_str().unwrap_or("")]);
            }

            cmd
        };

        // Redirect output
        if options.quiet {
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
        } else if let Some(log_path) = &options.log_path {
            let log_file = std::fs::File::create(log_path)
                .context("Failed to create validator log file")?;
            cmd.stdout(Stdio::from(log_file.try_clone()?));
            cmd.stderr(Stdio::from(log_file));
        }

        // Spawn the process
        let process = cmd.spawn().context("Failed to spawn validator process")?;

        let validator = Self {
            process: Some(process),
            ledger_dir,
            custom_ledger_path: options.ledger_path,
            rpc_url: format!("http://127.0.0.1:{}", options.rpc_port),
        };

        // Wait for validator to be ready
        validator
            .wait_ready(options.startup_timeout)
            .await
            .context("Validator failed to start")?;

        Ok(validator)
    }

    /// Wait for the validator to be ready (processing slots).
    ///
    /// Waits for slot > 0 which implies RPC is responding and programs are loaded.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        let client = reqwest::Client::new();

        loop {
            if start.elapsed() > timeout {
                bail!("Validator did not become ready within {:?}", timeout);
            }

            // Check if validator is processing slots (implies programs loaded)
            let result = client
                .post(&self.rpc_url)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSlot"
                }))
                .timeout(Duration::from_secs(2))
                .send()
                .await;

            if let Ok(resp) = result {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        if let Some(slot) = json.get("result").and_then(|r| r.as_u64()) {
                            if slot > 0 {
                                return Ok(());
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Get the RPC URL for this validator.
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    /// Check if the validator process is still running.
    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut process) = self.process {
            match process.try_wait() {
                Ok(None) => true, // Still running
                _ => false,       // Exited or error
            }
        } else {
            false
        }
    }

    /// Stop the validator.
    pub fn stop(&mut self) {
        if let Some(mut process) = self.process.take() {
            // Try graceful shutdown first
            #[cfg(unix)]
            {
                // SAFETY: sending SIGTERM to our child process
                unsafe {
                    libc::kill(process.id() as i32, libc::SIGTERM);
                }
                // Give it a moment to shut down gracefully
                std::thread::sleep(Duration::from_millis(500));
            }

            // Force kill if still running
            let _ = process.kill();
            let _ = process.wait();
        }
    }

    /// Check if something is listening on the given port.
    async fn is_port_in_use(port: u16) -> bool {
        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{}", port);

        client
            .post(&url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getHealth"
            }))
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .is_ok()
    }

    /// Kill any existing validator processes.
    ///
    /// Useful for cleanup before starting a new test.
    pub fn kill_existing() {
        #[cfg(unix)]
        {
            let _ = Command::new("pkill")
                .args(["-9", "-f", "solana-test-validator"])
                .output();
        }
    }
}

impl Drop for Validator {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Options for validator startup.
#[derive(Debug, Clone)]
pub struct ValidatorOptions {
    /// Use `make validator` instead of direct invocation.
    pub use_makefile: bool,
    /// RPC port (default: 8899).
    pub rpc_port: u16,
    /// Custom ledger path (uses temp dir if None).
    pub ledger_path: Option<PathBuf>,
    /// Path to write validator logs.
    pub log_path: Option<PathBuf>,
    /// Suppress validator output.
    pub quiet: bool,
    /// Timeout for validator startup.
    pub startup_timeout: Duration,
    /// Programs to deploy (program_id, .so path).
    pub programs: Vec<(String, PathBuf)>,
}

impl Default for ValidatorOptions {
    fn default() -> Self {
        Self {
            use_makefile: true,
            rpc_port: 8899,
            ledger_path: None,
            log_path: None,
            quiet: false,
            startup_timeout: Duration::from_secs(60),
            programs: Vec::new(),
        }
    }
}

impl ValidatorOptions {
    /// Create options for quiet operation (no output).
    pub fn quiet() -> Self {
        Self {
            quiet: true,
            ..Default::default()
        }
    }

    /// Set a custom RPC port.
    pub fn with_rpc_port(mut self, port: u16) -> Self {
        self.rpc_port = port;
        self
    }

    /// Set a custom ledger path.
    pub fn with_ledger_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ledger_path = Some(path.into());
        self
    }

    /// Set log output path.
    pub fn with_log_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.log_path = Some(path.into());
        self
    }

    /// Set startup timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Don't use makefile - invoke solana-test-validator directly.
    pub fn without_makefile(mut self) -> Self {
        self.use_makefile = false;
        self
    }

    /// Add a program to deploy.
    pub fn with_program(mut self, program_id: &str, so_path: impl Into<PathBuf>) -> Self {
        self.programs.push((program_id.to_string(), so_path.into()));
        self
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_workspace_root() {
        // Should find the tapedrive workspace root
        let root = find_workspace_root().unwrap();
        assert!(root.join("Cargo.toml").exists());
        assert!(root.join("e2e").exists() || root.join("cli").exists());
    }
}
