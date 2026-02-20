use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;

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

    pub fn runtime_mode(mut self, mode: NodeRuntimeMode) -> Self {
        self.config.runtime_mode = mode;
        self
    }

    pub fn base_port(mut self, base_port: u16) -> Self {
        self.config.base_port = base_port;
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

        tls::init_tls();

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
            let bind_addr = if self.config.base_port == 0 {
                tls::pick_bind(i as u64)?
            } else {
                let port = self
                    .config
                    .base_port
                    .saturating_add(i.try_into().unwrap_or(u16::MAX));
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
            };
            let port = bind_addr.port();

            nodes.push(TestNode::new(
                i,
                chain.rpc().clone(),
                self.config.runtime_mode,
                bind_addr,
                port,
                self.config.stop_timeout,
            )?);
        }

        Ok(SimnetHarness {
            config: self.config,
            chain,
            nodes,
        })
    }
}

impl Default for SimnetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory multi-node simulation harness.
pub struct SimnetHarness {
    config: SimnetConfig,
    chain: ChainFixture,
    nodes: Vec<TestNode>,
}

impl SimnetHarness {
    pub fn config(&self) -> &SimnetConfig {
        &self.config
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

    pub async fn start_all(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            node.start().await?;
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            node.stop().await?;
        }
        Ok(())
    }
}
