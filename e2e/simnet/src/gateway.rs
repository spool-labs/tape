use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use store_memory::MemoryStore;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_node::config::node::NodeConfig;
use tape_node::context::{NodeContext, NodeContextBuilder};
use tape_node::core::error::NodeError;
use tape_store::TapeStore;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::Instrument;

use crate::tls;

type TestGatewayContext = Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>;

/// One simulated read gateway with in-memory storage and a public HTTP server.
pub struct TestGateway {
    id: usize,
    public_host: IpAddr,
    public_port: u16,
    keypair: Keypair,
    bls_keypair: BlsPrivateKey,
    tls_keypair: CryptoKeypair,
    rpc: LiteSvmRpc,
    app_config: NodeConfig,
    context: Option<TestGatewayContext>,
    runtime: Option<JoinHandle<Result<(), NodeError>>>,
}

impl TestGateway {
    pub fn new(id: usize, rpc: LiteSvmRpc) -> Result<Self> {
        let bind_addr = tls::pick_bind(10_000 + id as u64)?;
        let public_port = bind_addr.port();
        let keypair = Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let tls_keypair = {
            let mut rng = rand::thread_rng();
            CryptoKeypair::new(&mut rng)
        };
        let public_host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let app_config = gateway_app_config(bind_addr)?;

        Ok(Self {
            id,
            public_host,
            public_port,
            keypair,
            bls_keypair,
            tls_keypair,
            rpc,
            app_config,
            context: None,
            runtime: None,
        })
    }

    pub fn id(&self) -> usize {
        self.id
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

    pub fn tls_pubkey(&self) -> NetworkTlsPubkey {
        NetworkTlsPubkey::new(self.tls_keypair.pubkey().to_bytes())
    }

    pub fn network_address(&self) -> NetworkAddress {
        NetworkAddress::new_ipv4([127, 0, 0, 1], self.public_port)
    }

    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.public_host, self.public_port)
    }

    pub fn context(&self) -> TestGatewayContext {
        self.context
            .as_ref()
            .cloned()
            .expect("gateway context not built; start runtime first")
    }

    pub fn is_running(&self) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| !runtime.is_finished())
    }

    pub async fn start(&mut self) -> Result<()> {
        if self.is_running() {
            return Ok(());
        }

        if self.context.is_none() {
            self.context = Some(self.build_context().await?);
        }

        let context = self.context();
        let config = self.app_config.clone();
        let id = self.id;
        let task = tokio::spawn(
            async move { tape_gateway::runtime::run_with_context(context, config).await }
                .instrument(tracing::info_span!("gateway", id)),
        );
        self.runtime = Some(task);
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(runtime) = self.runtime.take() {
            runtime.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), runtime).await;
        }
        Ok(())
    }

    async fn build_context(&self) -> Result<TestGatewayContext> {
        let store = TapeStore::new(MemoryStore::new());
        let rpc = RpcClient::from_rpc(self.rpc.clone());
        let peer_manager = Arc::new(PeerManager::new());
        let tls_identity = Arc::new(clone_ed25519_keypair(&self.tls_keypair));

        let api = Arc::new(
            peer_http::HttpApiBuilder::new()
                .local_identity(tls_identity.clone())
                .build(peer_manager.clone())
                .context("build gateway HttpApi")?,
        );

        let context = NodeContextBuilder::<MemoryStore, HttpApi, LiteSvmRpc>::new(
            self.app_config.clone(),
            clone_keypair(&self.keypair),
            self.bls_keypair.clone(),
            tls_identity,
            store,
            rpc,
            peer_manager,
            api,
        )
        .build()
        .await
        .context("build gateway context")?;

        Ok(context)
    }
}

fn gateway_app_config(bind_addr: SocketAddr) -> Result<NodeConfig> {
    let mut config = NodeConfig::default();
    config.node.node_keypair = PathBuf::from("/dev/null");
    config.node.bls_keypair = PathBuf::from("/dev/null");
    config.solana.rpc = vec!["http://127.0.0.1:8899".into()];
    config.solana.start_slot = None;
    config.http.listen = bind_addr;
    config.https.listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    config.store.path = PathBuf::from("/tmp");
    Ok(config)
}

fn clone_keypair(keypair: &Keypair) -> CryptoKeypair {
    CryptoKeypair::from_solana_keypair(keypair).expect("clone keypair")
}

fn clone_ed25519_keypair(keypair: &CryptoKeypair) -> CryptoKeypair {
    CryptoKeypair::from_keypair_bytes(keypair.to_keypair_bytes()).expect("clone ed25519 keypair")
}
