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
use tape_node2::core::channels::{downstream_channels, state_channel};
use tape_node2::core::config::{AppConfig, NodeConfig};
use tape_node2::core::context::{NodeContext, NodeContextBuilder};
use tape_node2::core::error::NodeError;
use tape_node2::features::block::ingestor::BlockIngestor;
use tape_node2::features::epoch::lifecycle::LifecycleWorker;
use tape_node2::features::epoch::manager::EpochManager;
use tape_node2::features::gc::manager::GcManager;
use tape_node2::features::http::server::HttpServer;
use tape_node2::features::replay::manager::ReplayManager;
use tape_node2::features::snapshot::manager::SnapshotManager;
use tape_node2::features::spool::manager::SpoolManager;
use tape_node2::features::state::manager::StateManager;
use tape_protocol::fetch::fetch_state;
use tape_store::{TapeStore, ops::MetaOps};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::NodeRuntimeMode;

type TestNodeContext = Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>;

struct RuntimeHandles {
    http: JoinHandle<Result<(), NodeError>>,
    block: JoinHandle<Result<(), NodeError>>,
    epoch: JoinHandle<Result<(), NodeError>>,
    lifecycle: JoinHandle<Result<(), NodeError>>,
    spool: JoinHandle<Result<(), NodeError>>,
    snapshot: JoinHandle<Result<(), NodeError>>,
    replay: JoinHandle<Result<(), NodeError>>,
    state: JoinHandle<Result<(), NodeError>>,
    gc: JoinHandle<Result<(), NodeError>>,
}

impl RuntimeHandles {
    async fn wait(self) {
        let _ = self.http.await;
        let _ = self.block.await;
        let _ = self.epoch.await;
        let _ = self.lifecycle.await;
        let _ = self.spool.await;
        let _ = self.snapshot.await;
        let _ = self.replay.await;
        let _ = self.state.await;
        let _ = self.gc.await;
    }

    fn abort(self) {
        self.http.abort();
        self.block.abort();
        self.epoch.abort();
        self.lifecycle.abort();
        self.spool.abort();
        self.snapshot.abort();
        self.replay.abort();
        self.state.abort();
        self.gc.abort();
    }
}

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

struct TestRuntime {
    cancel: CancellationToken,
    handles: RuntimeHandles,
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
    runtime: Option<TestRuntime>,
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
        self.runtime.is_some()
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.runtime.is_some() {
            return Ok(());
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
                let state = fetch_state(&context.rpc)
                    .await
                    .context("fetch initial protocol state")?;
                context
                    .set_state(state)
                    .context("publish initial protocol state")?;
                context
                    .refresh_peers()
                    .await
                    .context("refresh peers")?;

                let cancel = CancellationToken::new();
                let handles = spawn_runtime(context, &self.app_config, start_slot, cancel.clone());
                self.runtime = Some(TestRuntime { cancel, handles });
                Ok(())
            }
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(runtime) = self.runtime.take() {
            runtime.cancel.cancel();
            let _ = timeout(self.test_config.stop_timeout, runtime.handles.wait()).await;
        }

        Ok(())
    }

    /// Simulate a crash by aborting all runtime tasks immediately.
    pub fn kill(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.handles.abort();
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

fn spawn_runtime(
    context: TestNodeContext,
    config: &AppConfig,
    start_slot: SlotNumber,
    cancel: CancellationToken,
) -> RuntimeHandles {
    let (senders, receivers) = downstream_channels(&config.channels);
    let (state_tx, state_rx) = state_channel(&config.channels);
    let mut block_config = config.block.clone();
    block_config.start_slot = start_slot;

    RuntimeHandles {
        http: tokio::spawn(
            HttpServer::new(context.clone(), config.http.clone(), cancel.clone()).run(),
        ),
        block: tokio::spawn(
            BlockIngestor::new(
                context.clone(),
                block_config,
                senders,
                cancel.clone(),
            )
            .run(),
        ),
        epoch: tokio::spawn(
            EpochManager::new(
                context.clone(),
                config.epoch.clone(),
                receivers.epoch,
                cancel.clone(),
            )
            .run(),
        ),
        lifecycle: tokio::spawn(
            LifecycleWorker::new(
                context.clone(),
                config.epoch_lifecycle.clone(),
                cancel.clone(),
            )
            .run(),
        ),
        spool: tokio::spawn(
            SpoolManager::new(
                context.clone(),
                config.spool.clone(),
                cancel.clone(),
            )
            .run(),
        ),
        snapshot: tokio::spawn(
            SnapshotManager::new(
                context.clone(),
                config.snapshot.clone(),
                cancel.clone(),
            )
            .run(),
        ),
        replay: tokio::spawn(
            ReplayManager::new(
                context.clone(),
                config.replay.clone(),
                receivers.replay,
                state_tx,
                cancel.clone(),
            )
            .run(),
        ),
        state: tokio::spawn(
            StateManager::new(
                context.clone(),
                config.state.clone(),
                state_rx,
                cancel.clone(),
            )
            .run(),
        ),
        gc: tokio::spawn(GcManager::new(context, config.gc.clone(), cancel).run()),
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

    let mut config = AppConfig::production(node).context("build node2 app config")?;
    config.http.bind_addr = bind_addr;
    Ok(config)
}

fn clone_keypair(keypair: &Keypair) -> Keypair {
    Keypair::try_from(keypair.to_bytes().as_ref()).expect("clone keypair")
}
