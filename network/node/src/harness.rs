use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use peer_manager::PeerManager;
use peer_memory::MemoryApi;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use store_memory::MemoryStore;
use tape_chain_harness::{
    ChainHarness, ChainHarnessBuilder, HarnessNode, HarnessNodeSpec, IntoEpochNumber,
};
use tape_core::prelude::{EpochNumber, EpochPhase, SlotNumber, SpoolIndex, SpoolState, SpoolStatus};
use tape_crypto::ed25519::Keypair;
use tape_store::TapeStore;
use tape_store::ops::SpoolOps;

use crate::config::node::NodeConfig;
use crate::context::{NodeContext, NodeContextBuilder};

pub type TestContext = Arc<NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>>;

pub struct NodeHarness {
    chain: ChainHarness,
    contexts: Vec<TestContext>,
}

impl NodeHarness {
    pub fn builder() -> NodeHarnessBuilder {
        NodeHarnessBuilder::default()
    }

    pub async fn from_chain(chain: ChainHarness) -> Result<Self> {
        Self::from_chain_with_api(chain, None).await
    }

    pub async fn from_chain_with_api(
        chain: ChainHarness,
        api: Option<Arc<MemoryApi>>,
    ) -> Result<Self> {
        let mut contexts = Vec::new();

        for index in 0..chain.node_count() {
            let node = chain.node(index);
            let store = TapeStore::new(MemoryStore::new());
            let rpc = RpcClient::from_rpc(chain.rpc().clone());
            let peer_manager = Arc::new(PeerManager::new());
            let api = api.clone().unwrap_or_else(|| Arc::new(MemoryApi::noop()));

            let mut rng = rand::thread_rng();
            let tls = Arc::new(tape_crypto::ed25519::Keypair::new(&mut rng));

            let ctx = NodeContextBuilder::new(
                test_config(),
                clone_keypair(node.keypair()),
                node.bls_keypair().clone(),
                tls,
                store,
                rpc,
                peer_manager,
                api,
            )
            .build()
            .await?;

            ctx.set_state(chain.protocol_state().clone())?;
            contexts.push(ctx);
        }

        Ok(Self { chain, contexts })
    }

    pub fn ctx_for(&self, index: usize) -> TestContext {
        self.contexts[index].clone()
    }

    pub fn node(&self, index: usize) -> &HarnessNode {
        self.chain.node(index)
    }

    pub fn epoch(&self) -> EpochNumber {
        self.chain.epoch()
    }

    pub fn phase(&self) -> EpochPhase {
        self.chain.phase()
    }

    pub fn rpc(&self) -> &LiteSvmRpc {
        self.chain.rpc()
    }

    pub fn owned_spools(&self, index: usize) -> Vec<SpoolIndex> {
        self.chain.owned_spools(index)
    }

    pub fn set_spool_status(
        &self,
        index: usize,
        spool: SpoolIndex,
        status: SpoolStatus,
    ) -> Result<()> {
        let ctx = self
            .contexts
            .get(index)
            .ok_or_else(|| anyhow!("node index {index} out of range"))?;
        ctx.store
            .set_spool_state(spool, SpoolState::new(status, self.epoch()))
            .map_err(|error| anyhow!("set_spool_state({spool}): {error}"))
    }

    pub fn set_all_owned_spools_status(&self, index: usize, status: SpoolStatus) -> Result<()> {
        for spool in self.owned_spools(index) {
            self.set_spool_status(index, spool, status)?;
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct NodeHarnessBuilder {
    chain: ChainHarnessBuilder,
    api: Option<Arc<MemoryApi>>,
}

impl NodeHarnessBuilder {
    pub fn nodes(mut self, count: usize) -> Self {
        self.chain = self.chain.nodes(count);
        self
    }

    pub fn epoch(mut self, epoch: impl IntoEpochNumber) -> Self {
        self.chain = self.chain.epoch(epoch);
        self
    }

    pub fn phase(mut self, phase: EpochPhase) -> Self {
        self.chain = self.chain.phase(phase);
        self
    }

    pub fn last_epoch(mut self, timestamp: i64) -> Self {
        self.chain = self.chain.last_epoch(timestamp);
        self
    }

    pub fn time_elapsed(mut self) -> Self {
        self.chain = self.chain.time_elapsed();
        self
    }

    pub fn onchain_time_elapsed(mut self) -> Self {
        self.chain = self.chain.onchain_time_elapsed();
        self
    }

    pub fn current_committee_size(mut self, size: usize) -> Self {
        self.chain = self.chain.current_committee_size(size);
        self
    }

    pub fn prev_committee_size(mut self, size: usize) -> Self {
        self.chain = self.chain.prev_committee_size(size);
        self
    }

    pub fn next_committee_size(mut self, size: usize) -> Self {
        self.chain = self.chain.next_committee_size(size);
        self
    }

    pub fn current_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.chain = self.chain.current_committee_nodes(nodes);
        self
    }

    pub fn prev_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.chain = self.chain.prev_committee_nodes(nodes);
        self
    }

    pub fn next_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.chain = self.chain.next_committee_nodes(nodes);
        self
    }

    pub fn node<F>(mut self, index: usize, f: F) -> Self
    where
        F: FnOnce(&mut HarnessNodeSpec),
    {
        self.chain = self.chain.node(index, f);
        self
    }

    pub fn current_group_count(mut self, count: u64) -> Self {
        self.chain = self.chain.current_group_count(count);
        self
    }

    pub fn prev_group_count(mut self, count: u64) -> Self {
        self.chain = self.chain.prev_group_count(count);
        self
    }

    pub fn next_assignment_ready(mut self) -> Self {
        self.chain = self.chain.next_assignment_ready();
        self
    }

    pub fn candidate_ready(mut self) -> Self {
        self.chain = self.chain.candidate_ready();
        self
    }

    pub fn advance_ready(mut self) -> Self {
        self.chain = self.chain.advance_ready();
        self
    }

    pub fn no_prev_snapshot_tape(mut self) -> Self {
        self.chain = self.chain.no_prev_snapshot_tape();
        self
    }

    pub fn api(mut self, api: MemoryApi) -> Self {
        self.api = Some(Arc::new(api));
        self
    }

    pub async fn build(self) -> Result<NodeHarness> {
        let chain = self.chain.build().await?;
        NodeHarness::from_chain_with_api(chain, self.api).await
    }
}

pub use tape_chain_harness::{ChainFixture, HarnessSpec};

fn clone_keypair(keypair: &solana_sdk::signature::Keypair) -> Keypair {
    Keypair::from_solana_keypair(keypair).expect("clone keypair")
}

fn test_config() -> NodeConfig {
    let mut config = NodeConfig::default();
    config.node.node_keypair = PathBuf::from("/dev/null");
    config.node.bls_keypair = PathBuf::from("/dev/null");
    config.solana.rpc = "http://localhost:8899".into();
    config.solana.start_slot = Some(SlotNumber(0));
    config.store.path = PathBuf::from("/tmp");
    config
}
