use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};

use anyhow::{anyhow, Context, Result};
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::network::NetworkAddress;
use tape_node::runtime::{NodeApiConfig, NodeConfig, NodeContext, NodeContextBuilder, RecoveryConfig, TlsConfig};
use tape_node::runtime::{spawn_runtime, RuntimeHandles};
use tape_store::{MemoryStore, TapeStore};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use crate::config::NodeRuntimeMode;
use crate::tls;

struct TestTlsConfig {
    cert_path: PathBuf,
    key_path: PathBuf,
    _dir: PathBuf,
}

impl TestTlsConfig {
    fn new(id: usize, keypair: &Keypair) -> Result<Self> {
        let tls_dir = tls::temp_dir(&format!("tape-simnet-{id}"))?;
        let (cert_path, key_path) =
            tls::write_cert(keypair, &tls_dir, &format!("sim-node-{id}"))?;
        Ok(Self {
            cert_path,
            key_path,
            _dir: tls_dir,
        })
    }
}

struct TestNodeCtx {
    config: Option<NodeConfig>,
    keypair: Option<Keypair>,
    bls_keypair: BlsPrivateKey,
    rpc: Option<RpcClient<LiteSvmRpc>>,
    context: Option<Arc<NodeContext<MemoryStore, LiteSvmRpc>>>,
}

impl TestNodeCtx {
    fn new(
        id: usize,
        keypair: Keypair,
        rpc: LiteSvmRpc,
        bind_addr: SocketAddr,
        public_port: u16,
        tls_config: &TestTlsConfig,
    ) -> Result<Self> {
        let bls_keypair = BlsPrivateKey::from_random();
        let bls_path = tls_config._dir.join("bls.key");
        write_bls_keypair(&bls_path, &bls_keypair)?;
        let config = test_node_config(
            id,
            bind_addr,
            public_port,
            tls_config.cert_path.clone(),
            tls_config.key_path.clone(),
            bls_path,
        );

        Ok(Self {
            config: Some(config),
            keypair: Some(keypair),
            bls_keypair,
            rpc: Some(RpcClient::from_rpc(rpc)),
            context: None,
        })
    }
}

struct TestNodeStore {
    store: Option<TapeStore<MemoryStore>>,
}

impl TestNodeStore {
    fn new() -> Self {
        Self {
            store: Some(TapeStore::new(MemoryStore::new())),
        }
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

struct TestContext {
    cancel: Option<CancellationToken>,
    runtime: Option<RuntimeHandles>,
}

impl TestContext {
    fn new() -> Self {
        Self {
            cancel: None,
            runtime: None,
        }
    }
}

/// One simulated node with in-memory storage and optional runtime handles.
pub struct TestNode {
    id: usize,
    tls_config: TestTlsConfig,
    node_ctx: TestNodeCtx,
    node_store: TestNodeStore,
    test_config: TestConfig,
    test_context: TestContext,
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
        let tls_config = TestTlsConfig::new(id, &keypair)?;
        let node_ctx = TestNodeCtx::new(
            id,
            keypair,
            rpc,
            bind_addr,
            public_port,
            &tls_config,
        )?;
        let node_store = TestNodeStore::new();
        let test_config = TestConfig::new(mode, stop_timeout);
        let test_context = TestContext::new();

        Ok(Self {
            id,
            tls_config,
            node_ctx,
            node_store,
            test_config,
            test_context,
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn context(&self) -> Arc<NodeContext<MemoryStore, LiteSvmRpc>> {
        self.node_ctx.context
            .as_ref()
            .cloned()
            .expect("node context not built; start runtime first")
    }

    pub fn authority(&self) -> Pubkey {
        if let Some(context) = &self.node_ctx.context {
            context.keypair.pubkey()
        } else {
            self.node_ctx.keypair
                .as_ref()
                .expect("node keypair missing")
                .pubkey()
        }
    }

    pub fn keypair(&self) -> &Keypair {
        if let Some(context) = &self.node_ctx.context {
            context.keypair.as_ref()
        } else {
            self.node_ctx
                .keypair
                .as_ref()
                .expect("node keypair missing")
        }
    }

    pub fn bls_keypair(&self) -> &BlsPrivateKey {
        if let Some(context) = &self.node_ctx.context {
            context.bls_keypair.as_ref()
        } else {
            &self.node_ctx.bls_keypair
        }
    }

    pub fn network_address(&self) -> NetworkAddress {
        if let Some(context) = &self.node_ctx.context {
            NetworkAddress::new_ipv4([127, 0, 0, 1], context.config.public_port)
        } else {
            let config = self.node_ctx.config.as_ref().expect("node config missing");
            NetworkAddress::new_ipv4([127, 0, 0, 1], config.public_port)
        }
    }

    pub fn tls_cert_path(&self) -> PathBuf {
        self.tls_config.cert_path.clone()
    }

    pub fn tls_key_path(&self) -> PathBuf {
        self.tls_config.key_path.clone()
    }

    pub fn is_running(&self) -> bool {
        self.test_context.runtime.is_some()
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.test_context.runtime.is_some() {
            return Ok(());
        }

        match self.test_config.mode {
            NodeRuntimeMode::Disabled => Ok(()),
            NodeRuntimeMode::Full => {
                if self.node_ctx.context.is_none() {
                    let config = self
                        .node_ctx
                        .config
                        .take()
                        .ok_or_else(|| anyhow!("node config missing"))?;
                    let keypair = self
                        .node_ctx
                        .keypair
                        .take()
                        .ok_or_else(|| anyhow!("node keypair missing"))?;
                    let store = self
                        .node_store
                        .store
                        .take()
                        .ok_or_else(|| anyhow!("node store missing"))?;
                    let rpc = self
                        .node_ctx
                        .rpc
                        .take()
                        .ok_or_else(|| anyhow!("node rpc missing"))?;

                    let context = NodeContextBuilder::<MemoryStore, LiteSvmRpc>::new(config, keypair, store, rpc)
                        .build()
                        .await
                        .context("build node context")?;
                    self.node_ctx.context = Some(context);
                }

                let cancel = CancellationToken::new();
                let handles = spawn_runtime(self.context(), cancel.clone()).await;
                self.test_context.cancel = Some(cancel);
                self.test_context.runtime = Some(handles);
                Ok(())
            }
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(cancel) = self.test_context.cancel.take() {
            cancel.cancel();
        }

        if let Some(handles) = self.test_context.runtime.take() {
            let wait = async move {
                let _ = handles.ingestor.await;
                let _ = handles.fsm.await;
                let _ = handles.scheduler.await;
                let _ = handles.task_runner.await;
                let _ = handles.http.await;
            };
            let _ = timeout(self.test_config.stop_timeout, wait).await;
        }

        Ok(())
    }
}

fn test_node_config(
    id: usize,
    bind_addr: SocketAddr,
    public_port: u16,
    cert_path: PathBuf,
    key_path: PathBuf,
    bls_path: PathBuf,
) -> NodeConfig {
    NodeConfig {
        version: 1,
        name: format!("sim-node-{id}"),
        tls_keypair: PathBuf::from("/dev/null"),
        bls_keypair: bls_path,
        node_keypair: String::new(),
        bind_address: bind_addr,
        public_host: IpAddr::V4(Ipv4Addr::LOCALHOST).to_string(),
        public_port,
        tls: TlsConfig {
            certificate_path: Some(cert_path),
            key_path: Some(key_path),
            generate_self_signed: false,
        },
        storage_path: "/tmp".to_string(),
        poll_interval_ms: None,
        sync_concurrency: None,
        sync_batch_size: None,
        commission: None,
        recovery: RecoveryConfig::default(),
        node_api: NodeApiConfig::default(),
    }
}

fn write_bls_keypair(path: &std::path::Path, key: &BlsPrivateKey) -> io::Result<()> {
    let len = std::mem::size_of::<BlsPrivateKey>();
    let ptr = (key as *const BlsPrivateKey).cast::<u8>();
    // BlsPrivateKey is repr(C), fixed-size POD data written/read as raw bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    fs::write(path, bytes)
}
