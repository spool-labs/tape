use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use tape_core::encoding::ErasureCode;
use tape_core::thresholds::TrustModel;
use tokio::task::JoinHandle;

use crate::chain::ChainFixture;
use crate::config::{NodeRuntimeMode, SeededAccount, SimnetConfig};
use crate::log;
use crate::node::TestNode;
use crate::scenario::SimnetScenario;
use crate::tls;

/// Builder for a multi-node simnet harness.
#[derive(Debug, Clone)]
pub struct SimnetBuilder {
    config: SimnetConfig,
}

impl SimnetBuilder {
    pub fn new() -> Self {
        Self {
            config: SimnetConfig::default(),
        }
    }

    pub fn node_count(mut self, node_count: usize) -> Self {
        self.config.node_count = node_count;
        self
    }

    /// Create the chain on this profile and trust model; tapenet's by default.
    pub fn network(mut self, profile: ErasureCode, trust_model: TrustModel) -> Self {
        self.config.profile = profile;
        self.config.trust_model = trust_model;
        self
    }

    pub fn runtime_mode(mut self, mode: NodeRuntimeMode) -> Self {
        self.config.runtime_mode = mode;
        self
    }

    pub fn config(mut self, config: SimnetConfig) -> Self {
        self.config = config;
        self
    }

    pub fn slot_advance_per_tx(mut self, slots: u64) -> Self {
        self.config.slot_advance_per_tx = slots;
        self
    }

    /// Whether nodes propose and sign evictions, off by default.
    pub fn eviction(mut self, enabled: bool) -> Self {
        self.config.eviction.enabled = enabled;
        self
    }

    pub fn file_log(mut self, enabled: bool) -> Self {
        self.config.file_log = enabled;
        self
    }

    pub fn seed_account(
        mut self,
        address: impl Into<Pubkey>,
        owner: impl Into<Pubkey>,
        data: Vec<u8>,
    ) -> Self {
        self.config.seed_accounts.push(SeededAccount::new(address, owner, data));
        self
    }

    pub fn build(self) -> Result<SimnetHarness> {
        if self.config.node_count == 0 {
            return Err(anyhow!("node_count must be > 0"));
        }
        self.config
            .genesis()
            .map_err(|error| anyhow!("simnet profile: {error}"))?;

        if self.config.file_log {
            log::init_log();
            log::append_log("simnet builder start");
        }

        let chain = ChainFixture::new();
        for seed in &self.config.seed_accounts {
            chain
                .seed_account(&seed.address, &seed.owner, &seed.data)
                .with_context(|| {
                    format!("seed_account address={} owner={}", seed.address, seed.owner)
                })?;
        }

        let mut nodes = Vec::with_capacity(self.config.node_count);
        for i in 0..self.config.node_count {
            nodes.push(make_node(&self.config, &chain, i)?);
        }

        let admin = Keypair::new();

        Ok(SimnetHarness {
            config: self.config,
            chain,
            admin,
            nodes,
            block_producer: None,
        })
    }
}

impl Default for SimnetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// One block every slot, and each slot moves the chain clock by the same wall time
const SLOT_TIME: Duration = Duration::from_millis(300);

/// In-memory multi-node simulation harness.
pub struct SimnetHarness {
    config: SimnetConfig,
    chain: ChainFixture,
    admin: Keypair,
    nodes: Vec<TestNode>,
    block_producer: Option<JoinHandle<()>>,
}

impl SimnetHarness {
    pub fn config(&self) -> &SimnetConfig {
        &self.config
    }

    pub fn admin(&self) -> &Keypair {
        &self.admin
    }

    pub fn chain(&self) -> &ChainFixture {
        &self.chain
    }

    pub fn nodes(&self) -> &[TestNode] {
        &self.nodes
    }

    pub fn nodes_mut(&mut self) -> &mut [TestNode] {
        &mut self.nodes
    }

    pub fn node(&self, index: usize) -> Option<&TestNode> {
        self.nodes.get(index)
    }

    pub fn node_mut(&mut self, index: usize) -> Option<&mut TestNode> {
        self.nodes.get_mut(index)
    }

    pub fn scenario(&self) -> SimnetScenario<'_> {
        SimnetScenario::new(self)
    }

    pub fn add_node(&mut self) -> Result<usize> {
        let id = self.nodes.len();
        let node = make_node(&self.config, &self.chain, id)?;
        self.nodes.push(node);
        self.config.node_count = self.nodes.len();
        Ok(id)
    }

    pub async fn start_all(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            node.start().await?;
        }
        self.ensure_block_producer();
        Ok(())
    }

    pub async fn stop_nodes(&mut self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let node = self
                .nodes
                .get_mut(i)
                .ok_or_else(|| anyhow!("node {i} out of range"))?;
            node.stop().await?;
        }
        Ok(())
    }

    /// Simulate a crash on the given nodes by aborting their tasks immediately.
    pub fn kill_nodes(&mut self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let node = self
                .nodes
                .get_mut(i)
                .ok_or_else(|| anyhow!("node {i} out of range"))?;
            node.kill();
        }
        Ok(())
    }

    /// Give each stopped node an empty store, as if its drive was lost
    pub fn wipe_nodes(&mut self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let node = self
                .nodes
                .get_mut(i)
                .ok_or_else(|| anyhow!("node {i} out of range"))?;
            node.wipe()?;
        }
        Ok(())
    }

    pub async fn start_nodes(&mut self, indices: &[usize]) -> Result<()> {
        for &i in indices {
            let node = self
                .nodes
                .get_mut(i)
                .ok_or_else(|| anyhow!("node {i} out of range"))?;
            node.start().await?;
        }
        self.ensure_block_producer();
        Ok(())
    }

    pub async fn start_nodes_with_retry(
        &mut self,
        indices: &[usize],
        tries: usize,
        delay: Duration,
    ) -> Result<()> {
        let tries = tries.max(1);

        for _ in 0..tries {
            self.start_nodes(indices).await?;
            if indices.iter().all(|&i| {
                self.nodes
                    .get(i)
                    .is_some_and(|node| node.is_running())
            }) {
                return Ok(());
            }
            tokio::time::sleep(delay).await;
        }

        let failed: Vec<_> = indices
            .iter()
            .copied()
            .filter(|&i| {
                self.nodes
                    .get(i)
                    .is_none_or(|node| !node.is_running())
            })
            .collect();
        bail!("failed to start runtime on nodes: {failed:?}");
    }

    pub async fn stop_all(&mut self) -> Result<()> {
        if let Some(handle) = self.block_producer.take() {
            handle.abort();
        }
        let mut first_error = None;
        for node in &mut self.nodes {
            if let Err(error) = node.stop().await {
                first_error.get_or_insert(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn ensure_block_producer(&mut self) {
        if self.block_producer.is_none() {
            self.block_producer = Some(
                self.chain.rpc().start_block_producer(SLOT_TIME, SLOT_TIME),
            );
        }
    }
}

fn make_node(config: &SimnetConfig, chain: &ChainFixture, id: usize) -> Result<TestNode> {
    let bind_addr = tls::pick_bind()?;
    TestNode::new(id, chain.rpc().clone(), bind_addr, config)
}
