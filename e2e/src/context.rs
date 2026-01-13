//! Test context for e2e tests.
//!
//! Provides a builder pattern for setting up test environments with
//! validators, nodes, and common operations.
//!
//! # Example
//!
//! ```ignore
//! use tape_e2e::TestContext;
//!
//! #[tokio::test]
//! async fn test_example() {
//!     let ctx = TestContext::builder()
//!         .nodes(5)
//!         .stake(1000)
//!         .build_and_bootstrap()
//!         .await
//!         .unwrap();
//!
//!     // Test logic here - nodes are registered, staked, started, and bootstrapped
//!
//!     ctx.observe_epochs(10, |epoch, system| {
//!         println!("Epoch {}: phase={:?}", epoch.id.unwrap_or(0), epoch.phase);
//!         Ok(())
//!     }).await.unwrap();
//! }
//! ```

use std::time::Duration;

use anyhow::{Context as _, Result};

use crate::cli::{EpochAccount, SystemAccount};
use crate::consts::MIN_EPOCH_WAIT;
use crate::node::TestNode;
use crate::validator::{Validator, ValidatorOptions};
use crate::wait::{wait_for_epoch_advance_from, wait_for_rpc, LONG_TIMEOUT};
use crate::Tapedrive;

/// Test context containing validator, CLI, and nodes.
///
/// Created via the builder pattern. Handles cleanup automatically on drop.
pub struct TestContext {
    /// The local validator instance.
    pub validator: Validator,
    /// CLI wrapper for interacting with the system.
    pub cli: Tapedrive,
    /// Test nodes (may be empty if not using nodes).
    pub nodes: Vec<TestNode>,
    /// Whether nodes have been bootstrapped (activated in committee).
    bootstrapped: bool,
}

impl TestContext {
    /// Create a new builder for configuring the test context.
    pub fn builder() -> TestContextBuilder {
        TestContextBuilder::default()
    }

    /// Get the current epoch from the chain.
    pub fn epoch(&self) -> Result<EpochAccount> {
        self.cli.account_epoch()
    }

    /// Get the current system state from the chain.
    pub fn system(&self) -> Result<SystemAccount> {
        self.cli.account_system()
    }

    /// Manually advance the epoch (requires MIN_EPOCH_DURATION to have passed).
    pub fn advance_epoch(&self) -> Result<()> {
        self.cli.admin_advance_epoch()?;
        Ok(())
    }

    /// Wait for MIN_EPOCH_DURATION and then advance the epoch.
    pub async fn wait_and_advance_epoch(&self) -> Result<()> {
        tokio::time::sleep(MIN_EPOCH_WAIT).await;
        self.advance_epoch()
    }

    /// Observe epochs advancing autonomously.
    ///
    /// Waits for `count` epoch advances and calls `check` after each one.
    /// Useful for verifying system behavior over multiple epochs.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of epochs to observe
    /// * `check` - Callback to run after each epoch advance
    pub async fn observe_epochs<F>(&self, count: u64, mut check: F) -> Result<()>
    where
        F: FnMut(&EpochAccount, &SystemAccount) -> Result<()>,
    {
        let mut last_epoch_id = self.epoch()?.id.unwrap_or(0);
        let mut observed = 0u64;

        while observed < count {
            wait_for_epoch_advance_from(&self.cli, last_epoch_id, LONG_TIMEOUT)
                .await
                .context("Epoch should advance")?;

            let epoch = self.epoch()?;
            let system = self.system()?;
            let epoch_id = epoch.id.unwrap_or(0);

            observed += epoch_id - last_epoch_id;
            last_epoch_id = epoch_id;

            check(&epoch, &system)?;
        }

        Ok(())
    }

    /// Check all node logs for common errors.
    ///
    /// Returns an error if any node has errors like BadSpoolHash, BadEpochId, or panics.
    pub fn check_node_logs(&self) -> Result<()> {
        let mut errors = Vec::new();

        for node in &self.nodes {
            if let Ok(log) = node.read_log() {
                let has_bad_spool = log.contains("BadSpoolHash") || log.contains("0x54");
                let has_bad_epoch = log.contains("BadEpochId") || log.contains("0x43");
                let has_panic = log.contains("panic") || log.contains("PANIC");

                if has_bad_spool {
                    errors.push(format!("{}: BadSpoolHash", node.name));
                }
                if has_bad_epoch {
                    errors.push(format!("{}: BadEpochId", node.name));
                }
                if has_panic {
                    errors.push(format!("{}: panic", node.name));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("Node errors found: {}", errors.join(", "))
        }
    }

    /// Stop all nodes.
    pub fn stop_nodes(&mut self) {
        for node in &mut self.nodes {
            node.stop();
        }
    }

    /// Check if the system is in low-quorum mode.
    pub fn is_low_quorum(&self) -> Result<bool> {
        let system = self.system()?;
        let committee_size = system.committee_size.unwrap_or(0);
        Ok(committee_size < crate::consts::MIN_COMMITTEE_SIZE)
    }

    /// Get node URLs for all nodes.
    pub fn node_urls(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.url()).collect()
    }

    /// Add more nodes to the context.
    ///
    /// Registers, stakes, joins, funds, and starts the new nodes.
    /// Does NOT bootstrap them - they'll be activated on the next epoch advance.
    pub async fn add_nodes(&mut self, count: usize, stake: u64) -> Result<()> {
        let base_port = self.nodes.last().map(|n| n.port + 1).unwrap_or(10000);
        let start_index = self.nodes.len();

        for i in 0..count {
            let mut node = TestNode::new(start_index + i, base_port)
                .with_context(|| format!("Failed to create node {}", start_index + i))?;

            node.register(&self.cli)
                .with_context(|| format!("Failed to register node {}", start_index + i))?;
            node.stake(&self.cli, stake)
                .with_context(|| format!("Failed to stake node {}", start_index + i))?;
            node.join(&self.cli)
                .with_context(|| format!("Failed to join node {}", start_index + i))?;

            if let Err(e) = node.fund(&self.cli, 1.0) {
                eprintln!("Warning: Failed to fund node {}: {}", start_index + i, e);
            }

            let _ = node.start(&self.cli);
            self.nodes.push(node);
        }

        // Brief pause for nodes to initialize
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(())
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.stop_nodes();
    }
}

/// Builder for creating test contexts.
///
/// Use `TestContext::builder()` to create a new builder.
#[derive(Clone)]
pub struct TestContextBuilder {
    num_nodes: usize,
    base_port: u16,
    stake_amount: u64,
    fund_amount: f64,
    timeout: Duration,
    rpc_wait: Duration,
}

impl Default for TestContextBuilder {
    fn default() -> Self {
        Self {
            num_nodes: 0,
            base_port: 10000,
            stake_amount: 1000,
            fund_amount: 1.0,
            timeout: Duration::from_secs(300),
            rpc_wait: Duration::from_secs(30),
        }
    }
}

impl TestContextBuilder {
    /// Set the number of nodes to create.
    pub fn nodes(mut self, count: usize) -> Self {
        self.num_nodes = count;
        self
    }

    /// Set the base port for nodes (actual port = base_port + index).
    pub fn port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

    /// Set the stake amount for each node.
    pub fn stake(mut self, amount: u64) -> Self {
        self.stake_amount = amount;
        self
    }

    /// Set the SOL funding amount for each node.
    pub fn fund(mut self, amount: f64) -> Self {
        self.fund_amount = amount;
        self
    }

    /// Set the validator timeout.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }

    /// Set how long to wait for RPC to be ready.
    pub fn rpc_wait(mut self, duration: Duration) -> Self {
        self.rpc_wait = duration;
        self
    }

    /// Build the test context.
    ///
    /// This will:
    /// 1. Spawn a validator
    /// 2. Wait for RPC to be ready
    /// 3. Initialize the system
    /// 4. Create and register nodes (if num_nodes > 0)
    ///
    /// Nodes will be registered, staked, and joined but NOT started or bootstrapped.
    pub async fn build(self) -> Result<TestContext> {
        // Spawn validator
        let validator = Validator::spawn_with_options(
            ValidatorOptions::default().with_timeout(self.timeout),
        )
        .await
        .context("Failed to spawn validator")?;

        // Wait for RPC
        wait_for_rpc(validator.rpc_url(), self.rpc_wait)
            .await
            .context("Validator did not become ready")?;

        // Create CLI and initialize system
        let cli = Tapedrive::new_localnet();
        cli.admin_init().context("Failed to initialize system")?;

        // Create nodes
        let mut nodes = Vec::with_capacity(self.num_nodes);

        for i in 0..self.num_nodes {
            let mut node = TestNode::new(i, self.base_port)
                .with_context(|| format!("Failed to create node {}", i))?;

            node.register(&cli)
                .with_context(|| format!("Failed to register node {}", i))?;
            node.stake(&cli, self.stake_amount)
                .with_context(|| format!("Failed to stake node {}", i))?;
            node.join(&cli)
                .with_context(|| format!("Failed to join node {}", i))?;

            nodes.push(node);
        }

        Ok(TestContext {
            validator,
            cli,
            nodes,
            bootstrapped: false,
        })
    }

    /// Build and bootstrap the test context.
    ///
    /// This will:
    /// 1. Call `build()` to set up everything
    /// 2. Fund all nodes with SOL for transaction fees
    /// 3. Start all nodes
    /// 4. Wait for MIN_EPOCH_DURATION
    /// 5. Advance epoch to activate nodes (bootstrap)
    ///
    /// After this, nodes are in the committee and will advance epochs autonomously.
    pub async fn build_and_bootstrap(self) -> Result<TestContext> {
        let fund_amount = self.fund_amount;
        let mut ctx = self.build().await?;

        if ctx.nodes.is_empty() {
            return Ok(ctx);
        }

        // Fund nodes with SOL for transaction fees
        for (i, node) in ctx.nodes.iter().enumerate() {
            if let Err(e) = node.fund(&ctx.cli, fund_amount) {
                eprintln!("Warning: Failed to fund node {}: {}", i, e);
            }
        }

        // Start all nodes
        for (i, node) in ctx.nodes.iter_mut().enumerate() {
            if let Err(e) = node.start(&ctx.cli) {
                eprintln!("Warning: Failed to start node {}: {}", i, e);
            }
        }

        // Wait for nodes to initialize
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Bootstrap: advance epoch to activate nodes from committee_next to committee
        tokio::time::sleep(MIN_EPOCH_WAIT).await;
        ctx.cli
            .admin_advance_epoch()
            .context("Bootstrap epoch advance failed")?;

        ctx.bootstrapped = true;

        Ok(ctx)
    }
}

/// Varying stake amounts for testing stake weight effects.
pub const VARYING_STAKES: [u64; 5] = [100, 500, 1000, 2000, 5000];

/// Builder extension for creating nodes with varying stakes.
impl TestContextBuilder {
    /// Build with varying stake amounts per node.
    ///
    /// Uses predefined stake amounts: [100, 500, 1000, 2000, 5000].
    /// Number of nodes is determined by the length of VARYING_STAKES.
    pub async fn build_with_varying_stakes(mut self) -> Result<TestContext> {
        self.num_nodes = 0; // We'll create nodes manually

        // Spawn validator and init
        let validator = Validator::spawn_with_options(
            ValidatorOptions::default().with_timeout(self.timeout),
        )
        .await
        .context("Failed to spawn validator")?;

        wait_for_rpc(validator.rpc_url(), self.rpc_wait)
            .await
            .context("Validator did not become ready")?;

        let cli = Tapedrive::new_localnet();
        cli.admin_init().context("Failed to initialize system")?;

        // Create nodes with varying stakes
        let mut nodes = Vec::with_capacity(VARYING_STAKES.len());

        for (i, &stake) in VARYING_STAKES.iter().enumerate() {
            let mut node = TestNode::new(i, self.base_port)
                .with_context(|| format!("Failed to create node {}", i))?;

            node.register(&cli)
                .with_context(|| format!("Failed to register node {}", i))?;
            node.stake(&cli, stake)
                .with_context(|| format!("Failed to stake node {} with {}", i, stake))?;
            node.join(&cli)
                .with_context(|| format!("Failed to join node {}", i))?;

            nodes.push(node);
        }

        Ok(TestContext {
            validator,
            cli,
            nodes,
            bootstrapped: false,
        })
    }

    /// Build with varying stakes and bootstrap.
    pub async fn build_with_varying_stakes_and_bootstrap(self) -> Result<TestContext> {
        let fund_amount = self.fund_amount;
        let mut ctx = self.build_with_varying_stakes().await?;

        // Fund nodes
        for (i, node) in ctx.nodes.iter().enumerate() {
            if let Err(e) = node.fund(&ctx.cli, fund_amount) {
                eprintln!("Warning: Failed to fund node {}: {}", i, e);
            }
        }

        // Start nodes
        for (i, node) in ctx.nodes.iter_mut().enumerate() {
            if let Err(e) = node.start(&ctx.cli) {
                eprintln!("Warning: Failed to start node {}: {}", i, e);
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Bootstrap
        tokio::time::sleep(MIN_EPOCH_WAIT).await;
        ctx.cli
            .admin_advance_epoch()
            .context("Bootstrap epoch advance failed")?;

        ctx.bootstrapped = true;

        Ok(ctx)
    }
}
