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
//!         println!("Epoch {}: committee={}", epoch.id.as_u64(), system.committee.size());
//!         Ok(())
//!     }).await.unwrap();
//! }
//! ```

use std::time::Duration;

use anyhow::{Context as _, Result};
use solana_sdk::signature::Signer;

use crate::node::TestNode;
use crate::rpc::TestRpcClient;
use crate::validator::{Validator, ValidatorOptions};
use crate::wait::{wait_for_rpc, LONG_TIMEOUT};
use crate::Tapedrive;
use tape_api::prelude::{Epoch, System};
use tape_api::program::EPOCH_DURATION;
use tape_core::types::EpochNumber;

/// Test context containing validator, CLI, RPC client, and nodes.
///
/// Created via the builder pattern. Handles cleanup automatically on drop.
pub struct TestContext {
    /// The local validator instance.
    pub validator: Validator,
    /// CLI wrapper for mutations (register, stake, join, advance, etc.).
    pub cli: Tapedrive,
    /// RPC client for state queries.
    pub rpc: TestRpcClient,
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

    /// Get the current epoch from the chain via RPC.
    pub async fn epoch(&self) -> Result<Epoch> {
        self.rpc.get_epoch().await
    }

    /// Get the current epoch ID from the chain via RPC.
    pub async fn epoch_id(&self) -> Result<EpochNumber> {
        self.rpc.get_epoch_id().await
    }

    /// Get the current epoch phase from the chain via RPC.
    pub async fn epoch_phase(&self) -> Result<String> {
        self.rpc.get_epoch_phase().await
    }

    /// Get the current system state from the chain via RPC.
    pub async fn system(&self) -> Result<System> {
        self.rpc.get_system().await
    }

    /// Get the current committee size via RPC.
    pub async fn committee_size(&self) -> Result<usize> {
        self.rpc.get_committee_size().await
    }

    /// Get the current committee_next size via RPC.
    pub async fn committee_next_size(&self) -> Result<usize> {
        self.rpc.get_committee_next_size().await
    }

    /// Check if system is in bootstrap mode (committee_prev empty).
    pub async fn is_bootstrap_mode(&self) -> Result<bool> {
        self.rpc.is_bootstrap_mode().await
    }

    /// Manually advance the epoch (requires EPOCH_DURATION to have passed).
    pub fn advance_epoch(&self) -> Result<()> {
        self.cli.admin_advance_epoch()?;
        Ok(())
    }

    /// Wait for remaining EPOCH_DURATION and then advance the epoch.
    pub async fn wait_and_advance_epoch(&self) -> Result<()> {
        let wait = self.remaining_epoch_wait().await;
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        self.advance_epoch()
    }

    /// Calculate remaining time until EPOCH_DURATION has passed.
    async fn remaining_epoch_wait(&self) -> Duration {
        let epoch = self.rpc.get_epoch().await.ok();
        let last_epoch_ts = epoch.map(|e| e.last_epoch).unwrap_or(0);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let elapsed = now - last_epoch_ts;
        let remaining = (EPOCH_DURATION + 1) - elapsed;

        if remaining > 0 {
            Duration::from_secs(remaining as u64)
        } else {
            Duration::ZERO
        }
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
        F: FnMut(&Epoch, &System) -> Result<()>,
    {
        let mut last_epoch_id = self.epoch_id().await?.as_u64();
        let mut observed = 0u64;

        while observed < count {
            // Wait for epoch to advance
            crate::wait::wait_for_with_desc(
                &format!("epoch > {}", last_epoch_id),
                || async {
                    match self.epoch_id().await {
                        Ok(id) => Ok(id.as_u64() > last_epoch_id),
                        Err(_) => Ok(false),
                    }
                },
                LONG_TIMEOUT,
            )
            .await
            .context("Epoch should advance")?;

            let epoch = self.epoch().await?;
            let system = self.system().await?;
            let epoch_id = epoch.id.as_u64();

            observed += epoch_id - last_epoch_id;
            last_epoch_id = epoch_id;

            check(&epoch, &system)?;
        }

        Ok(())
    }

    /// Wait for the system to reach a specific epoch in Active phase.
    ///
    /// This is the primary helper for tests that need to reach epoch 4+
    /// where the system is fully operational (committee_prev is populated).
    ///
    /// Nodes advance epochs autonomously, so this just waits for them
    /// to reach the target epoch.
    ///
    /// # Arguments
    ///
    /// * `target` - The epoch number to wait for
    /// * `timeout` - Maximum time to wait
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Wait for epoch 4 (first epoch with full committee rotation)
    /// ctx.wait_for_epoch(EpochNumber(4), Duration::from_secs(120)).await?;
    /// ```
    pub async fn wait_for_epoch(&self, target: EpochNumber, timeout: Duration) -> Result<()> {
        // Wait for epoch ID to reach target
        crate::rpc::wait_for_epoch_id_rpc(&self.rpc, target, timeout)
            .await
            .with_context(|| format!("Epoch {} not reached", target.as_u64()))?;

        // Wait for Active phase
        crate::rpc::wait_for_epoch_phase_rpc(&self.rpc, "Active", timeout)
            .await
            .with_context(|| format!("Epoch {} did not reach Active phase", target.as_u64()))?;

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

    /// Check if the system would block epoch advancement.
    pub async fn would_block_advance(&self) -> Result<bool> {
        self.rpc.would_block_advance().await
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
    /// 4. Create RPC client
    /// 5. Create and register nodes (if num_nodes > 0)
    ///
    /// Nodes will be registered, staked, and joined but NOT started or bootstrapped.
    /// Node creation and on-chain registration run in parallel for speed.
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

        // Create RPC client
        let rpc = TestRpcClient::new(validator.rpc_url())
            .await
            .context("Failed to create RPC client")?;

        let num_nodes = self.num_nodes;
        let base_port = self.base_port;
        let stake_amount = self.stake_amount;

        let nodes = if num_nodes > 0 {
            // Run full node setup pipeline (create → register → stake → join) in parallel
            // Each task handles one node independently
            let node_futures: Vec<_> = (0..num_nodes)
                .map(|i| {
                    let bp = base_port;
                    let cli_clone = cli.clone();
                    let stake = stake_amount;
                    tokio::task::spawn_blocking(move || -> Result<TestNode> {
                        let mut node = TestNode::new(i, bp)
                            .with_context(|| format!("Failed to create node {}", i))?;
                        node.register(&cli_clone)
                            .with_context(|| format!("Failed to register node {}", i))?;
                        node.stake(&cli_clone, stake)
                            .with_context(|| format!("Failed to stake node {}", i))?;
                        node.join(&cli_clone)
                            .with_context(|| format!("Failed to join node {}", i))?;
                        Ok(node)
                    })
                })
                .collect();

            // Collect all results
            let mut created_nodes = Vec::with_capacity(num_nodes);
            for (i, fut) in node_futures.into_iter().enumerate() {
                let node = fut.await
                    .with_context(|| format!("Node {} task panicked", i))?
                    .with_context(|| format!("Node {} setup failed", i))?;
                created_nodes.push(node);
            }

            created_nodes
        } else {
            Vec::new()
        };

        Ok(TestContext {
            validator,
            cli,
            rpc,
            nodes,
            bootstrapped: false,
        })
    }

    /// Build and bootstrap the test context.
    ///
    /// This will:
    /// 1. Call `build()` to set up everything
    /// 2. Fund all nodes with SOL for transaction fees (in parallel)
    /// 3. Start all nodes
    /// 4. Wait for EPOCH_DURATION
    /// 5. Advance epoch to activate nodes (bootstrap)
    ///
    /// After this, nodes are in the committee and will advance epochs autonomously.
    pub async fn build_and_bootstrap(self) -> Result<TestContext> {
        let fund_amount = self.fund_amount;
        let mut ctx = self.build().await?;

        if ctx.nodes.is_empty() {
            return Ok(ctx);
        }

        // Fund all nodes in parallel
        let fund_futures: Vec<_> = ctx.nodes.iter().enumerate().map(|(i, node)| {
            let cli_clone = ctx.cli.clone();
            let pubkey = node.authority.pubkey();
            tokio::task::spawn_blocking(move || {
                cli_clone.transfer_sol(&pubkey, fund_amount)
                    .map_err(|e| (i, e))
            })
        }).collect();

        for fut in fund_futures {
            if let Err((i, e)) = fut.await.unwrap_or(Err((0, anyhow::anyhow!("task panicked")))) {
                eprintln!("Warning: Failed to fund node {}: {}", i, e);
            }
        }

        // Start all nodes (spawning processes) - do this sequentially to avoid process spawn issues
        for (i, node) in ctx.nodes.iter_mut().enumerate() {
            if let Err(e) = node.start(&ctx.cli) {
                eprintln!("Warning: Failed to start node {}: {}", i, e);
            }
        }

        // Wait for nodes to initialize
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Bootstrap: wait for nodes to advance the epoch autonomously
        // (they will call AdvanceEpoch once EPOCH_DURATION elapses)
        // Calculate remaining wait time - epoch clock started at admin_init, so
        // some/all of EPOCH_DURATION may have already elapsed during node setup
        let wait = ctx.remaining_epoch_wait().await;
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        // Wait for nodes to advance the epoch (don't call manually - nodes handle it)
        crate::rpc::wait_for_epoch_id_rpc(&ctx.rpc, EpochNumber(1), Duration::from_secs(30))
            .await
            .context("Nodes did not advance epoch during bootstrap")?;

        // Wait for epoch to reach Active phase (goes through Syncing -> Settling -> Active)
        crate::rpc::wait_for_epoch_phase_rpc(&ctx.rpc, "Active", Duration::from_secs(120))
            .await
            .context("Epoch did not reach Active phase after bootstrap")?;

        ctx.bootstrapped = true;

        Ok(ctx)
    }

    /// Build, bootstrap, and wait for a specific epoch.
    ///
    /// This is the primary builder for tests that need the system to be
    /// fully operational (epoch 4+). It:
    /// 1. Calls `build_and_bootstrap()` to set up and activate nodes
    /// 2. Waits for the system to reach the target epoch in Active phase
    ///
    /// # Arguments
    ///
    /// * `target_epoch` - The epoch to wait for (e.g., 4 for full committee rotation)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ctx = TestContext::builder()
    ///     .nodes(5)
    ///     .build_and_bootstrap_to_epoch(EpochNumber(4))
    ///     .await?;
    ///
    /// // System is now in epoch 4+ with committee_prev populated
    /// ```
    pub async fn build_and_bootstrap_to_epoch(self, target_epoch: EpochNumber) -> Result<TestContext> {
        let timeout = self.timeout;
        let ctx = self.build_and_bootstrap().await?;

        // Wait for target epoch
        ctx.wait_for_epoch(target_epoch, timeout)
            .await
            .with_context(|| format!("Failed to reach epoch {}", target_epoch.as_u64()))?;

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

        // Create RPC client
        let rpc = TestRpcClient::new(validator.rpc_url())
            .await
            .context("Failed to create RPC client")?;

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
            rpc,
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

        // Bootstrap: wait for nodes to advance the epoch autonomously
        let wait = ctx.remaining_epoch_wait().await;
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        // Wait for nodes to advance the epoch (don't call manually - nodes handle it)
        crate::rpc::wait_for_epoch_id_rpc(&ctx.rpc, EpochNumber(1), Duration::from_secs(30))
            .await
            .context("Nodes did not advance epoch during bootstrap")?;

        // Wait for epoch to reach Active phase
        crate::rpc::wait_for_epoch_phase_rpc(&ctx.rpc, "Active", Duration::from_secs(120))
            .await
            .context("Epoch did not reach Active phase after bootstrap")?;

        ctx.bootstrapped = true;

        Ok(ctx)
    }
}
