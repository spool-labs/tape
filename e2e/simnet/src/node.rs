use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use store_memory::MemoryStore;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::SlotNumber;
use tape_core::types::network::NetworkAddress;
use tape_node2::config::{AppConfig, NodeConfig};
use tape_node2::context::{NodeContext, NodeContextBuilder};
use tape_node2::runtime::{NodeRuntimeHandle, start_with_context};
use tape_store::{TapeStore, ops::MetaOps};
use tokio::time::Duration;

use crate::config::NodeRuntimeMode;

type TestNodeContext = Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>;

struct TestConfig {
    mode: NodeRuntimeMode,
    stop_timeout: Duration,
}

impl TestConfig {
    fn new(mode: NodeRuntimeMode, stop_timeout: Duration) -> Self {
        Self {
            mode,
            stop_timeout,
        }
    }
}

/// One simulated node with in-memory storage and optional runtime handles.
pub struct TestNode {
    id: usize,
    name: String,
    bind_addr: SocketAddr,
    public_host: IpAddr,
    public_port: u16,
    keypair: Keypair,
    bls_keypair: BlsPrivateKey,
    rpc: LiteSvmRpc,
    app_config: AppConfig,
    context: Option<TestNodeContext>,
    test_config: TestConfig,
    runtime: Option<NodeRuntimeHandle>,
}

impl TestNode {
    pub fn new(
        id: usize,
        rpc: LiteSvmRpc,
        mode: NodeRuntimeMode,
        bind_addr: SocketAddr,
        public_port: u16,
        stop_timeout: Duration,
    ) -> Result<Self> {
        let keypair = Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let name = format!("sim-node-{id}");
        let public_host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let app_config = test_app_config(bind_addr)?;

        Ok(Self {
            id,
            name,
            bind_addr,
            public_host,
            public_port,
            keypair,
            bls_keypair,
            rpc,
            app_config,
            context: None,
            test_config: TestConfig::new(mode, stop_timeout),
            runtime: None,
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn public_host(&self) -> IpAddr {
        self.public_host
    }

    pub fn public_port(&self) -> u16 {
        self.public_port
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.public_host, self.public_port)
    }

    pub fn context(&self) -> TestNodeContext {
        self.context
            .as_ref()
            .cloned()
            .expect("node context not built; start runtime first")
    }

    pub fn authority(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    pub fn bls_keypair(&self) -> &BlsPrivateKey {
        &self.bls_keypair
    }

    pub fn network_address(&self) -> NetworkAddress {
        NetworkAddress::new_ipv4([127, 0, 0, 1], self.public_port)
    }

    pub fn is_running(&self) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| !runtime.is_finished())
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.runtime.as_ref().is_some_and(|runtime| !runtime.is_finished()) {
            return Ok(());
        }

        if let Some(runtime) = self.runtime.take() {
            let _ = runtime.wait().await;
        }

        match self.test_config.mode {
            NodeRuntimeMode::Disabled => Ok(()),
            NodeRuntimeMode::Full => {
                if self.context.is_none() {
                    self.context = Some(self.build_context().await?);
                }

                let context = self.context();
                let start_slot = match context
                    .store
                    .get_sync_cursor()
                    .context("read sync cursor")?
                {
                    Some(slot) => SlotNumber(slot.0.saturating_add(1)),
                    None => SlotNumber(
                        context
                            .rpc
                            .get_slot()
                            .await
                            .context("read current chain slot")?
                            .saturating_add(1),
                    ),
                };
                let config = self
                    .app_config
                    .clone()
                    .with_block_start_slot(start_slot);

                self.runtime = Some(
                    start_with_context(context, config)
                        .await
                        .context("start supervised node2 runtime")?,
                );
                Ok(())
            }
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(runtime) = self.runtime.take() {
            runtime
                .shutdown(self.test_config.stop_timeout)
                .await
                .context("shutdown node2 runtime")?;
        }

        Ok(())
    }

    /// Simulate a crash by aborting all runtime tasks immediately.
    pub fn kill(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.abort();
        }
    }

    async fn build_context(&self) -> Result<TestNodeContext> {
        let store = TapeStore::new(MemoryStore::new());
        let rpc = RpcClient::from_rpc(self.rpc.clone());
        let peer_manager = Arc::new(PeerManager::new());
        let api = Arc::new(HttpApi::with_default_timeouts(peer_manager.clone()));

        let context = NodeContextBuilder::<MemoryStore, HttpApi, LiteSvmRpc>::new(
            self.app_config.node.clone(),
            clone_keypair(&self.keypair),
            self.bls_keypair.clone(),
            store,
            rpc,
            peer_manager,
            api,
        )
        .build()
        .await
        .context("build node2 context")?;

        Ok(context)
    }
}

fn test_app_config(bind_addr: SocketAddr) -> Result<AppConfig> {
    let node = NodeConfig {
        node_keypair: String::new(),
        bls_keypair: PathBuf::from("/dev/null"),
        rpc_url: "http://127.0.0.1:8899".into(),
        storage_path: "/tmp".into(),
        start_slot: SlotNumber(1),
    };

    AppConfig::testing(node, bind_addr).context("build node2 app config")
}

fn clone_keypair(keypair: &Keypair) -> Keypair {
    Keypair::try_from(keypair.to_bytes().as_ref()).expect("clone keypair")
}
