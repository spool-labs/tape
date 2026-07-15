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
use tape_gateway::admission::{AdmitAll, Admission};
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_node::config::gateway::WriteDefault;
use tape_node::config::node::NodeConfig;
use tape_node::context::{NodeContext, NodeContextBuilder};
use tape_node::core::error::NodeError;
use tape_store::TapeStore;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::Instrument;

use crate::tls;

type TestGatewayContext = Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>;

/// Maximum attempts to pick a loopback port distinct from the ports the gateway
/// runtime already binds before giving up.
const MAX_PORT_PICK_ATTEMPTS: u64 = 16;
const S3_PORT_BASE: u64 = 20_000;
const ADMIN_PORT_BASE: u64 = 40_000;

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
    s3_listen: Option<SocketAddr>,
    s3_admin_listen: Option<SocketAddr>,
    admission: Option<Arc<dyn Admission>>,
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
            s3_listen: None,
            s3_admin_listen: None,
            admission: None,
            context: None,
            runtime: None,
        })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    /// Enable the S3-compatible listener on its own loopback port and return the
    /// bound address. Must be called before [`start`](Self::start).
    ///
    /// Configures only the read surface: no delegate key and no SigV4 credential
    /// are set, so the listener serves anonymous GET/HEAD/ListObjectsV2 while any
    /// write is rejected (and the admin control plane stays disabled). The port is
    /// chosen distinct from the public read-gateway port, since the runtime binds
    /// both when it starts.
    pub fn enable_s3(&mut self) -> Result<SocketAddr> {
        let mut s3_addr = tls::pick_bind(S3_PORT_BASE + self.id as u64)?;
        let mut attempts = 0u64;
        while s3_addr.port() == self.public_port {
            attempts += 1;
            if attempts > MAX_PORT_PICK_ATTEMPTS {
                anyhow::bail!("could not pick an s3 port distinct from the public read port");
            }
            s3_addr = tls::pick_bind(S3_PORT_BASE + self.id as u64 + attempts)?;
        }

        self.app_config.gateway.s3.enabled = true;
        self.app_config.gateway.s3.listen = s3_addr;
        self.s3_listen = Some(s3_addr);
        Ok(s3_addr)
    }

    /// Enable the S3 listener together with the delegate-signed *write* path.
    ///
    /// Extends [`enable_s3`](Self::enable_s3) by wiring the three things a live
    /// write needs:
    /// - **Delegate signer**: this gateway's own keypair is written to a
    ///   Solana-format JSON keypair file and `gateway.s3.delegate_key` is pointed
    ///   at it, so the gateway signs on-chain `TrackWrite`/`DeleteTrack` as a
    ///   delegate whose pubkey is exactly [`authority`](Self::authority) — the
    ///   value the target tape must set as its on-chain `delegate` (via
    ///   `set_tape_delegate`) for `Tape::is_operator` to admit the signature.
    /// - **Bootstrap SigV4 credential**: `access_key_id`/`secret_access_key` so a
    ///   signed request can be verified (anonymous reads still pass; unsigned
    ///   writes are rejected).
    /// - **Write default**: `gateway.s3.write.default = Allow` so the verified
    ///   bootstrap credential is admitted without a stored policy rule. The admin
    ///   control plane is left disabled (no operator token), and the authorization
    ///   chokepoint still fails closed for anonymous/unsigned writes.
    ///
    /// Must be called before [`start`](Self::start). Returns the bound S3 address.
    pub fn enable_s3_writes(
        &mut self,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<SocketAddr> {
        let s3_addr = self.enable_s3()?;
        let path = self.write_delegate_keyfile(s3_addr.port())?;

        self.app_config.gateway.s3.delegate_key = Some(path);
        self.app_config.gateway.s3.access_key_id = Some(access_key_id.to_string());
        self.app_config.gateway.s3.secret_access_key = Some(secret_access_key.to_string());
        self.app_config.gateway.s3.write.default = WriteDefault::Allow;

        Ok(s3_addr)
    }

    /// Enable the S3 listener with the delegate-signed write path **and** the
    /// write-authorization admin control plane, under a fail-closed
    /// **default-deny** policy.
    ///
    /// Extends [`enable_s3_writes`](Self::enable_s3_writes) for the full
    /// authorization flow exercised by the live admin/credential/kill-switch test:
    /// - **Delegate signer + bootstrap SigV4 credential**: identical to
    ///   [`enable_s3_writes`](Self::enable_s3_writes) (the gateway's own keypair
    ///   signs on-chain writes as the tape's delegate; `access_key_id` /
    ///   `secret_access_key` make a SigV4 signature *verifiable*).
    /// - **`write.default = Deny`**: a verified credential is admitted only when a
    ///   stored policy rule explicitly allows it. With no credential and no rule,
    ///   even a correctly-signed write is denied (fail-closed).
    /// - **`write.pepper`**: required for the admin API to issue store credentials
    ///   (a credential's secret is persisted only as `HMAC-SHA256(secret,
    ///   pepper)`).
    /// - **`write.admin.operator_token`** on its own loopback port (the returned
    ///   `admin_addr`): starts the admin control-plane listener, authenticated by
    ///   this operator bearer token, for issuing/revoking credentials, editing
    ///   policy, and flipping the global write kill switch.
    ///
    /// Returns `(s3_addr, admin_addr)`. Must be called before
    /// [`start`](Self::start).
    pub fn enable_s3_admin_writes(
        &mut self,
        access_key_id: &str,
        secret_access_key: &str,
        operator_token: &str,
        pepper: &str,
    ) -> Result<(SocketAddr, SocketAddr)> {
        let s3_addr = self.enable_s3()?;
        let path = self.write_delegate_keyfile(s3_addr.port())?;

        self.app_config.gateway.s3.delegate_key = Some(path);
        self.app_config.gateway.s3.access_key_id = Some(access_key_id.to_string());
        self.app_config.gateway.s3.secret_access_key = Some(secret_access_key.to_string());
        self.app_config.gateway.s3.write.default = WriteDefault::Deny;
        self.app_config.gateway.s3.write.pepper = Some(pepper.to_string());
        self.app_config.gateway.s3.write.admin.operator_token = Some(operator_token.to_string());

        // The admin control plane binds its own listener (the runtime binds the
        // public read port, the S3 data port, and this admin port). Pick a port
        // distinct from the other two.
        let mut admin_addr = tls::pick_bind(ADMIN_PORT_BASE + self.id as u64)?;
        let mut attempts = 0u64;
        while admin_addr.port() == self.public_port || admin_addr.port() == s3_addr.port() {
            attempts += 1;
            if attempts > MAX_PORT_PICK_ATTEMPTS {
                anyhow::bail!(
                    "could not pick an s3 admin port distinct from the public and s3 ports"
                );
            }
            admin_addr = tls::pick_bind(ADMIN_PORT_BASE + self.id as u64 + attempts)?;
        }
        self.app_config.gateway.s3.write.admin.listen = admin_addr;
        self.s3_admin_listen = Some(admin_addr);

        Ok((s3_addr, admin_addr))
    }

    /// Persist this gateway's keypair as a Solana-format JSON keypair file (a JSON
    /// array of the 64 keypair bytes) so the S3 write context can load it as the
    /// delegate signer. The derived pubkey equals [`authority`](Self::authority),
    /// the value the target tape must delegate to.
    fn write_delegate_keyfile(&self, tag: u16) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!(
            "tape-s3-delegate-{}-{}.json",
            std::process::id(),
            tag
        ));
        let bytes = self.keypair.to_bytes();
        let mut json = String::from("[");
        for (index, byte) in bytes.iter().enumerate() {
            if index > 0 {
                json.push(',');
            }
            json.push_str(&byte.to_string());
        }
        json.push(']');
        std::fs::write(&path, json).context("write s3 delegate keypair file")?;
        Ok(path)
    }

    /// Inject the admission gate the gateway runtime starts with; without one
    /// the runtime admits every write. Must be called before start.
    pub fn set_admission(&mut self, admission: Arc<dyn Admission>) {
        self.admission = Some(admission);
    }

    /// Base URL of the S3-compatible listener (`http://127.0.0.1:{port}`).
    ///
    /// Panics if [`enable_s3`](Self::enable_s3) was not called first.
    pub fn s3_base_url(&self) -> String {
        let addr = self
            .s3_listen
            .expect("s3 listener not enabled; call enable_s3() before start()");
        format!("http://{addr}")
    }

    /// Base URL of the S3 write-authorization admin control plane
    /// (`http://127.0.0.1:{port}`).
    ///
    /// Panics if [`enable_s3_admin_writes`](Self::enable_s3_admin_writes) was not
    /// called first.
    pub fn s3_admin_base_url(&self) -> String {
        let addr = self.s3_admin_listen.expect(
            "s3 admin control plane not enabled; call enable_s3_admin_writes() before start()",
        );
        format!("http://{addr}")
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
        let admission = match self.admission.clone() {
            Some(admission) => admission,
            None => Arc::new(AdmitAll),
        };
        let id = self.id;
        let task = tokio::spawn(
            async move { tape_gateway::runtime::run_with_context(context, config, admission).await }
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
