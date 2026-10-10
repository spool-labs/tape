use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tape_peer_http::HttpApi;
use tape_peer_manager::PeerManager;
use tape_rpc::{Rpc, RpcError};
use tape_rpc_client::RpcClient;
use tape_rpc_solana::{RpcConfig, SolanaRpc};
use tape_api::program::tapedrive::node_pda;
use tape_api::state::Node;
use tape_api::utils::to_name;
use tape_core::bls::BlsPrivateKey;
use tape_core::system::{NodeKind, NodePreferences};
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair;
use tape_store::TapeStore;
use tracing::{info, warn};

use crate::chain::register_node::submit_register_node;
use crate::chain::set_network_tls::submit_set_network_tls;
use crate::config::node::NodeConfig;
use crate::context::{fetch_network_params, AppContext, NodeContextBuilder, NodeStore};
use crate::core::atlas::{self, AtlasBuffer};
use crate::core::error::NodeError;
use crate::core::restarts;

/// The lock file the reel keeps in a volume's root
const REEL_LOCK_FILE: &str = "reel.lock";

/// The extension of a reel segment file
const REEL_SEGMENT_EXTENSION: &str = "reel";

/// The file RocksDB keeps in every database directory
const ROCKS_CURRENT_FILE: &str = "CURRENT";

/// Whether a store root holds a reel volume
pub fn holds_reel_volume(root: &Path) -> bool {
    if root.join(REEL_LOCK_FILE).exists() {
        return true;
    }

    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        if entry.path().extension().is_some_and(|extension| extension == REEL_SEGMENT_EXTENSION) {
            return true;
        }
    }
    false
}

/// Whether a store root holds the two RocksDB volumes, or one of them
pub fn holds_rocks_store(root: &Path) -> bool {
    let meta = root.join(tape_store::config::META_SUBDIR).join(ROCKS_CURRENT_FILE);
    let bulk = root.join(tape_store::config::BULK_SUBDIR).join(ROCKS_CURRENT_FILE);
    meta.is_file() || bulk.is_file()
}

/// Refuse a store root that holds the other engine's store
///
/// The two engines keep their files under one root without touching each
/// other's. A node started on the wrong binary would open an empty store beside
/// the full one and sync everything again.
#[cfg(not(feature = "rocks"))]
fn refuse_other_engine(root: &Path) -> Result<(), NodeError> {
    match holds_rocks_store(root) {
        true => Err(NodeError::Store(format!(
            "store at {} holds a RocksDB store, and this binary serves the reel",
            root.display()
        ))),
        false => Ok(()),
    }
}

#[cfg(feature = "rocks")]
fn refuse_other_engine(root: &Path) -> Result<(), NodeError> {
    match holds_reel_volume(root) {
        true => Err(NodeError::Store(format!(
            "store at {} holds a reel volume, and this binary serves RocksDB",
            root.display()
        ))),
        false => Ok(()),
    }
}

#[cfg(not(feature = "rocks"))]
pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<NodeStore>, NodeError> {
    let root = &config.store.path;
    refuse_other_engine(root)?;

    // The first open replays the tail of every unsealed segment and rebuilds the
    // resident index, so the gap between these two lines can run minutes.
    info!(
        root = %root.display(),
        requested = ?config.store.io_backend,
        sync_bytes = config.store.sync_bytes,
        reserve = ?config.store.reserve,
        "opening store",
    );
    let opened_at = Instant::now();

    let store = tape_store_reel::open_node_store(
        root,
        tape_store_reel::NodeStoreOptions {
            compaction_mbps: config.store.compaction_mb_per_sec,
            sync_bytes: config.store.sync_bytes,
            backend: config.store.io_backend,
            reserve: config.store.reserve,
            segment_bytes: config.store.segment_bytes,
        },
    )
    .map_err(|error| {
        NodeError::Store(format!(
            "failed to open storage (root={}): {error}",
            root.display()
        ))
    })?;

    // The backend that took the volume beside the one the config asked for. The
    // two differ whenever a ring was configured and the kernel would not give
    // one, and this line is where an operator finds out.
    info!(
        elapsed_ms = opened_at.elapsed().as_millis() as u64,
        requested = ?config.store.io_backend,
        serving = %store.inner().inner().serving_backend(),
        "store opened",
    );

    Ok(store)
}

/// The same store on RocksDB, two volumes under the one configured root
///
/// The reel's own knobs have no opposite number here and are ignored. Compaction
/// is paced the same on both volumes, since one root means one device.
#[cfg(feature = "rocks")]
pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<NodeStore>, NodeError> {
    let root = &config.store.path;
    refuse_other_engine(root)?;

    let meta_dir = root.join(tape_store::config::META_SUBDIR);
    let bulk_dir = root.join(tape_store::config::BULK_SUBDIR);

    info!(
        root = %root.display(),
        compaction_mb_per_sec = config.store.compaction_mb_per_sec,
        "opening store",
    );
    let opened_at = Instant::now();

    let store = TapeStore::open_primary_split(
        &meta_dir,
        &bulk_dir,
        config.store.compaction_mb_per_sec,
        config.store.compaction_mb_per_sec,
    )
    .map_err(|error| {
        NodeError::Store(format!(
            "failed to open storage (root={}): {error}",
            root.display()
        ))
    })?;

    info!(
        elapsed_ms = opened_at.elapsed().as_millis() as u64,
        "store opened",
    );

    Ok(store)
}

/// Close the volume once every service has joined
///
/// Syncs the tails and journals, and trims each segment to its written end. A
/// failure is only logged, since the process is on its way out.
#[cfg(not(feature = "rocks"))]
pub fn close_primary_store(store: &TapeStore<NodeStore>) {
    let started = Instant::now();
    match store.inner().inner().close() {
        Ok(()) => info!(elapsed_ms = started.elapsed().as_millis() as u64, "store closed"),
        Err(error) => warn!(error = %error, "store close failed"),
    }
}

/// RocksDB closes itself when the last handle drops
#[cfg(feature = "rocks")]
pub fn close_primary_store(_store: &TapeStore<NodeStore>) {}

fn build_rpc_client(config: &NodeConfig) -> Result<RpcClient<SolanaRpc>, NodeError> {
    let rpc_config = RpcConfig {
        endpoints: config.solana.rpc.clone(),
        strategy: config.solana.rpc_strategy,
        ..RpcConfig::default()
    };

    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        return RpcClient::new_with_metrics(rpc_config).map_err(NodeError::from);
    }

    RpcClient::new(rpc_config).map_err(NodeError::from)
}

fn build_peer_api(
    #[cfg_attr(not(feature = "metrics"), allow(unused))]
    config: &NodeConfig,
    peer_manager: Arc<PeerManager>,
    tls_identity: Arc<Keypair>,
    atlas: Arc<AtlasBuffer>,
) -> Result<Arc<HttpApi>, NodeError> {
    let mut builder = tape_peer_http::HttpApiBuilder::new().local_identity(tls_identity);
    if atlas.enabled() {
        builder = builder.transfer_sink(Arc::new(move |transfer: tape_peer_http::PeerTransfer| {
            atlas.push_transfer(transfer.node, transfer.op, transfer.sent, transfer.bytes);
        }));
    }

    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        if let Some(registry) = tape_metrics::MetricsRegistry::get() {
            let metrics = Arc::new(
                tape_peer_http::ApiMetrics::new(registry.prometheus_registry()),
            );
            let api = builder.metrics(metrics).build(peer_manager)?;
            return Ok(Arc::new(api));
        }
    }

    let api = builder.build(peer_manager)?;
    Ok(Arc::new(api))
}

fn init_metrics(config: &NodeConfig) {
    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        tape_metrics::MetricsRegistry::init();
        tape_store_api::metrics::init_metrics();
        info!("prometheus metrics initialized");
        return;
    }

    #[cfg(not(feature = "metrics"))]
    if config.metrics.enabled {
        tracing::warn!(
            "metrics.enabled = true but binary was built without the 'metrics' feature"
        );
    }
}

pub async fn build_context(config: &NodeConfig, kind: NodeKind) -> Result<AppContext, NodeError> {
    let keypair = config.load_node_keypair()?;
    let bls_keypair = config.load_bls_keypair()?;
    let tls_keypair = config.load_or_generate_tls_keypair()?;

    init_metrics(config);

    let store = open_primary_store(config)?;
    // After the open, which is what creates the root the tally sits in.
    restarts::record(&config.store.path);

    let rpc = build_rpc_client(config)?;

    ensure_registered(config, kind, &rpc, &keypair, &bls_keypair, &tls_keypair).await?;

    let peer_manager = Arc::new(PeerManager::new());
    let tls_identity = Arc::new(tls_keypair);

    let observers = atlas::parse_observers(&config.https.observers)
        .map_err(NodeError::Config)?;
    let atlas = Arc::new(AtlasBuffer::new(observers));

    let api = build_peer_api(config, peer_manager.clone(), tls_identity.clone(), atlas.clone())?;

    NodeContextBuilder::new(
        config.clone(),
        keypair,
        bls_keypair,
        tls_identity,
        store,
        rpc,
        peer_manager,
        api,
        atlas,
    )
    .build()
    .await
}

pub fn resolve_network_address(config: &NodeConfig) -> Result<NetworkAddress, NodeError> {
    if let Some(host) = &config.network.host {
        if let Ok(ip) = host.parse::<IpAddr>() {
            let addr = SocketAddr::new(ip, config.network.port);
            return Ok(NetworkAddress::from_socket_addr(addr));
        }

        return NetworkAddress::new_domain(host, config.network.port)
            .map_err(|error| NodeError::Config(format!("invalid network.host: {error}")));
    }

    if !config.http.listen.ip().is_unspecified() {
        return Ok(NetworkAddress::from_socket_addr(config.http.listen));
    }

    Err(NodeError::Config(
        "cannot determine advertised address: set network.host to an IP address or hostname \
         or use a concrete http.listen address"
            .into(),
    ))
}

async fn reconcile_network_tls<Blockchain: Rpc>(
    config: &NodeConfig,
    rpc: &RpcClient<Blockchain>,
    authority: &Keypair,
    local_tls_pubkey: NetworkTlsPubkey,
    on_chain: NetworkTlsPubkey,
) -> Result<(), NodeError> {
    if on_chain == local_tls_pubkey {
        return Ok(());
    }

    if !config.https.auto_update {
        return Err(NodeError::Config(format!(
            "on-chain network_tls {on_chain} does not match local TLS keypair {local_tls_pubkey}; \
             rotate on-chain via SetNetworkTls, or enable https.auto_update to overwrite"
        )));
    }

    let (node_address, _) = node_pda(authority.address());

    info!(
        local = %local_tls_pubkey,
        on_chain = %on_chain,
        node = %node_address,
        "updating on-chain network_tls to match local keypair (https.auto_update=true)"
    );

    submit_set_network_tls(rpc, authority, node_address, local_tls_pubkey)
        .await
        .map_err(NodeError::Rpc)?;

    Ok(())
}

fn validate_node_metadata(
    node: &Node,
    config: &NodeConfig,
    bls_keypair: &BlsPrivateKey,
) -> Result<(), NodeError> {
    let local_bls = bls_keypair
        .public_key()
        .map_err(|e| NodeError::Keypair(format!("bls public key: {e:?}")))?;

    if node.metadata.bls_pubkey != local_bls {
        return Err(NodeError::Config(
            "on-chain BLS key does not match local BLS keypair".into(),
        ));
    }

    let expected_address = resolve_network_address(config)?;
    if node.metadata.network_address != expected_address {
        return Err(NodeError::Config(
            "on-chain network address does not match config; \
             update on-chain address via SetNetworkAddress or fix config"
                .into(),
        ));
    }

    Ok(())
}

pub async fn ensure_registered<Blockchain: Rpc>(
    config: &NodeConfig,
    kind: NodeKind,
    rpc: &RpcClient<Blockchain>,
    keypair: &Keypair,
    bls_keypair: &BlsPrivateKey,
    tls_keypair: &Keypair,
) -> Result<(), NodeError> {
    let authority = keypair.address();
    let local_tls_pubkey = NetworkTlsPubkey::new(tls_keypair.pubkey().to_bytes());

    match rpc.get_node(&authority).await {
        Ok(node) => {
            info!(authority = %authority, "node already registered on-chain");
            validate_node_metadata(&node, config, bls_keypair)?;
            reconcile_network_tls(
                config,
                rpc,
                keypair,
                local_tls_pubkey,
                node.metadata.network_tls,
            )
            .await?;
            return Ok(());
        }
        Err(RpcError::AccountNotFound(_)) => {}
        Err(err) => return Err(NodeError::Rpc(err)),
    }

    let network_address = resolve_network_address(config)?;

    let bls_pubkey = bls_keypair
        .public_key()
        .map_err(|e| NodeError::Keypair(format!("bls public key: {e:?}")))?;
    let bls_pop = bls_keypair
        .proof_of_possession()
        .map_err(|e| NodeError::Keypair(format!("bls proof of possession: {e:?}")))?;

    let name = to_name(&config.node.name);
    let commission = config.node.commission;

    // A custom preset takes its profile from the chain the node is joining.
    let params = fetch_network_params(rpc).await?;
    let genesis = config
        .genesis_preset
        .config(&params)
        .map_err(|error| NodeError::Config(format!("genesis preset: {error}")))?;

    info!(
        name = %config.node.name,
        authority = %authority,
        network_tls = %local_tls_pubkey,
        "registering node on-chain"
    );

    let result = submit_register_node(
        rpc,
        keypair,
        kind,
        name,
        commission,
        network_address,
        local_tls_pubkey,
        bls_pubkey,
        bls_pop,
        NodePreferences::from(&genesis),
    )
    .await;

    match result {
        Ok(txid) => {
            info!(%txid, "node registered successfully");
            Ok(())
        }
        Err(reg_err) => {
            // Registration failed, re-fetch to handle concurrent registration.
            match rpc.get_node(&authority).await {
                Ok(node) => {
                    info!("node appeared on-chain after failed registration tx");
                    validate_node_metadata(&node, config, bls_keypair)?;
                    if node.metadata.network_tls != local_tls_pubkey {
                        warn!(
                            on_chain = %node.metadata.network_tls,
                            local = %local_tls_pubkey,
                            "node appeared on-chain with different TLS key; attempting reconcile"
                        );
                        reconcile_network_tls(
                            config,
                            rpc,
                            keypair,
                            local_tls_pubkey,
                            node.metadata.network_tls,
                        )
                        .await?;
                    }
                    Ok(())
                }
                Err(_) => Err(NodeError::Rpc(reg_err)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;

    use tape_rpc_client::RpcClient;
    use tape_api::genesis::GenesisConfig;
    use tape_api::utils::to_name;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::system::{NodeKind, NodePreferences};
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_core::types::{BasisPoints, EpochNumber, SlotNumber};
    use tape_crypto::ed25519::Keypair;

    use super::{ensure_registered, resolve_network_address};
    use crate::chain::register_node::submit_register_node;
    use crate::config::node::NodeConfig;
    use crate::core::error::NodeError;
    use crate::harness::NodeHarness;

    fn tls_pubkey(kp: &Keypair) -> NetworkTlsPubkey {
        NetworkTlsPubkey::new(kp.pubkey().to_bytes())
    }

    fn test_config_with_address(ip: [u8; 4], port: u16) -> NodeConfig {
        let mut config = NodeConfig::default();
        config.node.name = "test-node".into();
        config.node.node_keypair = PathBuf::from("/dev/null");
        config.node.bls_keypair = PathBuf::from("/dev/null");
        config.solana.rpc = vec!["http://localhost:8899".into()];
        config.solana.start_slot = Some(SlotNumber(0));
        config.store.path = PathBuf::from("/tmp");
        config.network.host = Some(format!(
            "{}.{}.{}.{}",
            ip[0], ip[1], ip[2], ip[3]
        ));
        config.network.port = port;
        config
    }

    // the node's own store opens on the reel, under the shipped defaults
    #[cfg(not(feature = "rocks"))]
    #[test]
    fn opens_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::default();
        config.store.path = dir.path().join("volume");

        let store = super::open_primary_store(&config).expect("open the node store");
        let opened = store.inner().inner().engine().config();

        assert!(opened.map_above.is_none());
        assert_eq!(
            opened.sync,
            reel::SyncPolicy::Bytes(reel::ByteCount::from_bytes(16 * 1024 * 1024)),
        );
    }

    // a rocks build puts its two volumes under the one configured root
    #[cfg(feature = "rocks")]
    #[test]
    fn opens_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::default();
        config.store.path = dir.path().join("volume");

        super::open_primary_store(&config).expect("open the node store");

        assert!(config.store.path.join(tape_store::config::META_SUBDIR).is_dir());
        assert!(config.store.path.join(tape_store::config::BULK_SUBDIR).is_dir());
    }

    // each engine's files are told apart, and an empty root holds neither
    #[test]
    fn engine_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let empty = dir.path().join("empty");
        let locked = dir.path().join("locked");
        let segmented = dir.path().join("segmented");
        let rocks = dir.path().join("rocks");
        let meta = rocks.join(tape_store::config::META_SUBDIR);
        for root in [&empty, &locked, &segmented, &meta] {
            std::fs::create_dir_all(root).expect("root");
        }
        std::fs::write(locked.join("reel.lock"), b"").expect("lock");
        std::fs::write(segmented.join("000001.reel"), b"").expect("segment");
        std::fs::write(meta.join("CURRENT"), b"").expect("current");

        assert!(!super::holds_reel_volume(&empty) && !super::holds_rocks_store(&empty));
        assert!(super::holds_reel_volume(&locked) && !super::holds_rocks_store(&locked));
        assert!(super::holds_reel_volume(&segmented));
        assert!(super::holds_rocks_store(&rocks) && !super::holds_reel_volume(&rocks));
    }

    // the reel build refuses a root a RocksDB node wrote, and leaves it alone
    #[cfg(not(feature = "rocks"))]
    #[test]
    fn refuses_rocks_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::default();
        config.store.path = dir.path().join("volume");
        let meta = config.store.path.join(tape_store::config::META_SUBDIR);
        std::fs::create_dir_all(&meta).expect("meta");
        std::fs::write(meta.join("CURRENT"), b"").expect("current");

        let opened = super::open_primary_store(&config);

        assert!(opened.is_err_and(|error| error.to_string().contains("RocksDB")));
        assert!(!super::holds_reel_volume(&config.store.path));
    }

    // the RocksDB build refuses a root a reel node wrote, and leaves it alone
    #[cfg(feature = "rocks")]
    #[test]
    fn refuses_reel_volume() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::default();
        config.store.path = dir.path().join("volume");
        std::fs::create_dir_all(&config.store.path).expect("volume");
        std::fs::write(config.store.path.join("reel.lock"), b"").expect("lock");

        let opened = super::open_primary_store(&config);

        assert!(opened.is_err_and(|error| error.to_string().contains("reel")));
        assert!(!super::holds_rocks_store(&config.store.path));
    }

    // -- resolve_network_address tests --

    #[test]
    fn resolves_host_ip() {
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let addr = resolve_network_address(&config).expect("resolve");
        let expected = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );
        assert_eq!(addr, expected);
    }

    #[test]
    fn resolves_hostname_host() {
        let mut config = NodeConfig::default();
        config.node.name = "test".into();
        config.network.host = Some("example.com".into());
        config.network.port = 443;

        let addr = resolve_network_address(&config).expect("resolve");
        let expected = NetworkAddress::new_domain("example.com", 443).expect("domain");
        assert_eq!(addr, expected);
    }

    #[test]
    fn resolves_listen_fallback() {
        let mut config = NodeConfig::default();
        config.node.name = "test".into();
        config.network.host = None;
        config.http.listen = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            3000,
        );
        let addr = resolve_network_address(&config).expect("resolve");
        let expected = NetworkAddress::from_socket_addr(config.http.listen);
        assert_eq!(addr, expected);
    }

    #[test]
    fn rejects_unspecified_address() {
        let mut config = NodeConfig::default();
        config.node.name = "test".into();
        config.network.host = None;
        config.http.listen = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            3000,
        );
        let err = resolve_network_address(&config).unwrap_err();
        assert!(matches!(err, NodeError::Config(_)));
    }

    // -- ensure_registered tests --

    async fn register_fresh_node(
        harness: &NodeHarness,
        keypair: &Keypair,
        bls: &BlsPrivateKey,
        address: NetworkAddress,
        tls_pubkey: NetworkTlsPubkey,
    ) {
        let authority = keypair.pubkey().into();
        harness
            .rpc()
            .airdrop(&authority, 10_000_000_000)
            .expect("airdrop");

        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        submit_register_node(
            &rpc,
            keypair,
            NodeKind::Storage,
            to_name("test-node"),
            BasisPoints(0),
            address,
            tls_pubkey,
            bls.public_key().expect("bls pubkey"),
            bls.proof_of_possession().expect("bls pop"),
            NodePreferences::from(&GenesisConfig::simnet()),
        )
        .await
        .expect("register node");
    }

    #[tokio::test]
    async fn registers_new_node() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let authority = keypair.pubkey().into();
        harness
            .rpc()
            .airdrop(&authority, 10_000_000_000)
            .expect("airdrop");

        let bls = BlsPrivateKey::from_random();
        let tls = Keypair::new(&mut rng);
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());

        ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls, &tls)
            .await
            .expect("ensure_registered");

        let node = rpc.get_node(&keypair.address()).await.expect("get node");
        assert_eq!(
            node.metadata.bls_pubkey,
            bls.public_key().expect("bls pubkey")
        );
        assert_eq!(node.metadata.network_tls, tls_pubkey(&tls));
    }

    #[tokio::test]
    async fn skips_existing_node_when_tls_matches() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls = BlsPrivateKey::from_random();
        let tls = Keypair::new(&mut rng);
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address, tls_pubkey(&tls)).await;

        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls, &tls)
            .await
            .expect("ensure_registered idempotent");
    }

    #[tokio::test]
    async fn auto_updates_network_tls_on_mismatch() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls = BlsPrivateKey::from_random();
        let old_tls = Keypair::new(&mut rng);
        let new_tls = Keypair::new(&mut rng);
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address, tls_pubkey(&old_tls)).await;

        let mut config = test_config_with_address([10, 0, 0, 1], 443);
        config.https.auto_update = true;
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls, &new_tls)
            .await
            .expect("auto-update");

        let node = rpc.get_node(&keypair.address()).await.expect("get node");
        assert_eq!(node.metadata.network_tls, tls_pubkey(&new_tls));
    }

    #[tokio::test]
    async fn rejects_tls_mismatch_when_auto_update_disabled() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls = BlsPrivateKey::from_random();
        let old_tls = Keypair::new(&mut rng);
        let new_tls = Keypair::new(&mut rng);
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address, tls_pubkey(&old_tls)).await;

        let mut config = test_config_with_address([10, 0, 0, 1], 443);
        config.https.auto_update = false;
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let err = ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls, &new_tls)
            .await
            .unwrap_err();

        match err {
            NodeError::Config(msg) => assert!(msg.contains("network_tls")),
            other => panic!("expected Config error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn rejects_bls_mismatch() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls_original = BlsPrivateKey::from_random();
        let tls = Keypair::new(&mut rng);
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls_original, address, tls_pubkey(&tls)).await;

        let bls_different = BlsPrivateKey::from_random();
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let err = ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls_different, &tls)
            .await
            .unwrap_err();

        match err {
            NodeError::Config(msg) => assert!(msg.contains("BLS key")),
            other => panic!("expected Config error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn rejects_address_mismatch() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls = BlsPrivateKey::from_random();
        let tls = Keypair::new(&mut rng);
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address, tls_pubkey(&tls)).await;

        let config = test_config_with_address([10, 0, 0, 2], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let err = ensure_registered(&config, NodeKind::Storage, &rpc,&keypair, &bls, &tls)
            .await
            .unwrap_err();

        match err {
            NodeError::Config(msg) => assert!(msg.contains("network address")),
            other => panic!("expected Config error, got: {other}"),
        }
    }
}
