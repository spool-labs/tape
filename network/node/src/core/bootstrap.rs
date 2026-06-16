use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use store_rocks::RocksStore;
use tape_api::program::tapedrive::node_pda;
use tape_api::state::Node;
use tape_api::utils::to_name;
use tape_core::bls::BlsPrivateKey;
use tape_core::system::NodePreferences;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair;
use tape_store::TapeStore;
use tracing::{info, warn};

use crate::chain::register_node::submit_register_node;
use crate::chain::set_network_tls::submit_set_network_tls;
use crate::config::node::NodeConfig;
use crate::context::{AppContext, NodeContextBuilder};
use crate::core::error::NodeError;

pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<RocksStore>, NodeError> {
    TapeStore::open_primary_with_compaction_rate_limit(
        &config.store.path,
        config.store.compaction_mb_per_sec,
    )
    .map_err(|error| {
        NodeError::Store(format!(
            "failed to open storage at {}: {error}",
            config.store.path.display()
        ))
    })
}

fn build_rpc_client(config: &NodeConfig) -> Result<RpcClient<SolanaRpc>, NodeError> {
    let rpc_config = RpcConfig {
        endpoints: vec![config.solana.rpc.clone()],
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
) -> Result<Arc<HttpApi>, NodeError> {
    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        if let Some(registry) = tape_metrics::MetricsRegistry::get() {
            let metrics = Arc::new(
                peer_http::ApiMetrics::new(registry.prometheus_registry()),
            );
            let api = peer_http::HttpApiBuilder::new()
                .metrics(metrics)
                .local_identity(tls_identity)
                .build(peer_manager)?;
            return Ok(Arc::new(api));
        }
    }

    let api = peer_http::HttpApiBuilder::new()
        .local_identity(tls_identity)
        .build(peer_manager)?;
    Ok(Arc::new(api))
}

fn init_metrics(config: &NodeConfig) {
    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        tape_metrics::MetricsRegistry::init();
        store::metrics::init_metrics();
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

pub async fn build_context(config: &NodeConfig) -> Result<AppContext, NodeError> {
    let keypair = config.load_node_keypair()?;
    let bls_keypair = config.load_bls_keypair()?;
    let tls_keypair = config.load_or_generate_tls_keypair()?;

    init_metrics(config);

    let store = open_primary_store(config)?;
    let rpc = build_rpc_client(config)?;

    ensure_registered(config, &rpc, &keypair, &bls_keypair, &tls_keypair).await?;

    let peer_manager = Arc::new(PeerManager::new());
    let tls_identity = Arc::new(tls_keypair);

    let api = build_peer_api(config, peer_manager.clone(), tls_identity.clone())?;

    NodeContextBuilder::new(
        config.clone(),
        keypair,
        bls_keypair,
        tls_identity,
        store,
        rpc,
        peer_manager,
        api,
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

    info!(
        name = %config.node.name,
        authority = %authority,
        network_tls = %local_tls_pubkey,
        "registering node on-chain"
    );

    let result = submit_register_node(
        rpc,
        keypair,
        name,
        commission,
        network_address,
        local_tls_pubkey,
        bls_pubkey,
        bls_pop,
        NodePreferences::from(&config.genesis_preset.config()),
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

    use rpc_client::RpcClient;
    use tape_api::genesis::GenesisConfig;
    use tape_api::utils::to_name;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::system::NodePreferences;
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
        config.solana.rpc = "http://localhost:8899".into();
        config.solana.start_slot = Some(SlotNumber(0));
        config.store.path = PathBuf::from("/tmp");
        config.network.host = Some(format!(
            "{}.{}.{}.{}",
            ip[0], ip[1], ip[2], ip[3]
        ));
        config.network.port = port;
        config
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
        let authority = keypair.to_solana_pubkey();
        harness
            .rpc()
            .airdrop(&authority, 10_000_000_000)
            .expect("airdrop");

        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        submit_register_node(
            &rpc,
            keypair,
            to_name("test-node"),
            BasisPoints(0),
            address,
            tls_pubkey,
            bls.public_key().expect("bls pubkey"),
            bls.proof_of_possession().expect("bls pop"),
            NodePreferences::from(&GenesisConfig::local()),
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
        let authority = keypair.to_solana_pubkey();
        harness
            .rpc()
            .airdrop(&authority, 10_000_000_000)
            .expect("airdrop");

        let bls = BlsPrivateKey::from_random();
        let tls = Keypair::new(&mut rng);
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());

        ensure_registered(&config, &rpc, &keypair, &bls, &tls)
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
        ensure_registered(&config, &rpc, &keypair, &bls, &tls)
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
        ensure_registered(&config, &rpc, &keypair, &bls, &new_tls)
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
        let err = ensure_registered(&config, &rpc, &keypair, &bls, &new_tls)
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
        let err = ensure_registered(&config, &rpc, &keypair, &bls_different, &tls)
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
        let err = ensure_registered(&config, &rpc, &keypair, &bls, &tls)
            .await
            .unwrap_err();

        match err {
            NodeError::Config(msg) => assert!(msg.contains("network address")),
            other => panic!("expected Config error, got: {other}"),
        }
    }
}
