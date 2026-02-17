use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::network::NetworkAddress;
use tape_node::core::config::RecoveryConfig;
use tape_node::core::{NodeApiConfig, NodeConfig, NodeContext, TlsConfig};
use tape_node::pipeline::{spawn_runtime, RuntimeHandles};
use tape_store::{MemoryStore, TapeStore};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;

use crate::config::NodeRuntimeMode;
use crate::tls;

/// One simulated node with in-memory storage and optional runtime handles.
pub struct NodeFixture {
    id: usize,
    mode: NodeRuntimeMode,
    stop_timeout: Duration,
    context: Arc<NodeContext<MemoryStore, LiteSvmRpc>>,
    cancel: Option<CancellationToken>,
    runtime: Option<RuntimeHandles>,
    tls_cert_path: PathBuf,
    tls_key_path: PathBuf,
    _tls_dir: PathBuf,
}

impl NodeFixture {
    pub fn new(
        id: usize,
        rpc: LiteSvmRpc,
        mode: NodeRuntimeMode,
        bind_addr: SocketAddr,
        public_port: u16,
        stop_timeout: Duration,
    ) -> Result<Self> {
        let keypair = Keypair::new();
        let bls = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        let tls_dir = tls::temp_dir(&format!("tape-simnet-{id}"))?;
        let (cert_path, key_path) = tls::write_cert(&keypair, &tls_dir, &format!("sim-node-{id}"))?;
        let config = test_node_config(
            id,
            bind_addr,
            public_port,
            cert_path.clone(),
            key_path.clone(),
        );

        let context = NodeContext::new(
            config,
            keypair,
            bls,
            store,
            RpcClient::from_rpc(rpc),
        );

        Ok(Self {
            id,
            mode,
            stop_timeout,
            context,
            cancel: None,
            runtime: None,
            tls_cert_path: cert_path,
            tls_key_path: key_path,
            _tls_dir: tls_dir,
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn context(&self) -> Arc<NodeContext<MemoryStore, LiteSvmRpc>> {
        self.context.clone()
    }

    pub fn authority(&self) -> Pubkey {
        self.context.keypair.pubkey()
    }

    pub fn keypair(&self) -> &Keypair {
        self.context.keypair.as_ref()
    }

    pub fn bls_keypair(&self) -> &BlsPrivateKey {
        self.context.bls_keypair.as_ref()
    }

    pub fn network_address(&self) -> NetworkAddress {
        NetworkAddress::new_ipv4([127, 0, 0, 1], self.context.config.public_port)
    }

    pub fn tls_cert_path(&self) -> PathBuf {
        self.tls_cert_path.clone()
    }

    pub fn tls_key_path(&self) -> PathBuf {
        self.tls_key_path.clone()
    }

    pub fn is_running(&self) -> bool {
        self.runtime.is_some()
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.runtime.is_some() {
            return Ok(());
        }

        match self.mode {
            NodeRuntimeMode::Disabled => Ok(()),
            NodeRuntimeMode::Full => {
                let cancel = CancellationToken::new();
                let handles = spawn_runtime(self.context.clone(), cancel.clone()).await;
                self.cancel = Some(cancel);
                self.runtime = Some(handles);
                Ok(())
            }
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }

        if let Some(handles) = self.runtime.take() {
            let wait = async move {
                let _ = handles.ingestor.await;
                let _ = handles.fsm.await;
                let _ = handles.reconciler.await;
                let _ = handles.supervisor.await;
                let _ = handles.http.await;
            };
            let _ = timeout(self.stop_timeout, wait).await;
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
) -> NodeConfig {
    NodeConfig {
        version: 1,
        name: format!("sim-node-{id}"),
        tls_keypair: PathBuf::from("/dev/null"),
        bls_keypair: PathBuf::from("/dev/null"),
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
