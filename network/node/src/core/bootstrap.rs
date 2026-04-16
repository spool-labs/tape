use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use store_rocks::RocksStore;
use tape_api::state::Node;
use tape_api::utils::to_name;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::network::NetworkAddress;
use tape_crypto::ed25519::Keypair;
use tape_store::TapeStore;
use tracing::info;

use crate::chain::register_node::submit_register_node;
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
) -> Result<Arc<HttpApi>, NodeError> {
    #[cfg(feature = "metrics")]
    if config.metrics.enabled {
        if let Some(registry) = tape_metrics::MetricsRegistry::get() {
            let metrics = Arc::new(
                peer_http::ApiMetrics::new(registry.prometheus_registry()),
            );
            let api = peer_http::HttpApiBuilder::new()
                .metrics(metrics)
                .build(peer_manager)?;
            return Ok(Arc::new(api));
        }
    }

    Ok(Arc::new(HttpApi::with_default_timeouts(peer_manager)))
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

    init_metrics(config);

    let store = open_primary_store(config)?;
    let rpc = build_rpc_client(config)?;

    ensure_registered(config, &rpc, &keypair, &bls_keypair).await?;

    let peer_manager = Arc::new(PeerManager::new());
    let api = build_peer_api(config, peer_manager.clone())?;

    NodeContextBuilder::new(
        config.clone(),
        keypair,
        bls_keypair,
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
        let ip: IpAddr = host.parse().map_err(|_| {
            NodeError::Config(
                "network.host must be an IP address, not a hostname".into(),
            )
        })?;
        let addr = SocketAddr::new(ip, config.network.port);
        return Ok(NetworkAddress::from_socket_addr(addr));
    }

    if !config.http.listen.ip().is_unspecified() {
        return Ok(NetworkAddress::from_socket_addr(config.http.listen));
    }

    Err(NodeError::Config(
        "cannot determine advertised address: set network.host to an IP address \
         or use a concrete http.listen address"
            .into(),
    ))
}

fn validate_node_metadata<Blockchain: Rpc>(
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
) -> Result<(), NodeError> {
    let authority = keypair.address();

    match rpc.get_node(&authority).await {
        Ok(node) => {
            info!(authority = %authority, "node already registered on-chain");
            return validate_node_metadata::<Blockchain>(&node, config, bls_keypair);
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
        "registering node on-chain"
    );

    let result = submit_register_node(
        rpc,
        keypair,
        name,
        commission,
        network_address,
        bls_pubkey,
        bls_pop,
    )
    .await;

    match result {
        Ok(txid) => {
            info!(txid = ?txid, "node registered successfully");
            Ok(())
        }
        Err(reg_err) => {
            // Registration failed, re-fetch to handle concurrent registration.
            match rpc.get_node(&authority).await {
                Ok(node) => {
                    info!("node appeared on-chain after failed registration tx");
                    validate_node_metadata::<Blockchain>(&node, config, bls_keypair)
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
    use tape_api::utils::to_name;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::{BasisPoints, EpochNumber, SlotNumber};
    use tape_crypto::ed25519::Keypair;

    use super::{ensure_registered, resolve_network_address};
    use crate::chain::register_node::submit_register_node;
    use crate::config::node::NodeConfig;
    use crate::core::error::NodeError;
    use crate::harness::NodeHarness;

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
    fn rejects_hostname_host() {
        let mut config = NodeConfig::default();
        config.node.name = "test".into();
        config.network.host = Some("example.com".into());
        config.network.port = 443;

        let err = resolve_network_address(&config).unwrap_err();
        match err {
            NodeError::Config(msg) => assert!(msg.contains("IP address")),
            other => panic!("expected Config error, got: {other}"),
        }
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
            bls.public_key().expect("bls pubkey"),
            bls.proof_of_possession().expect("bls pop"),
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
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());

        ensure_registered(&config, &rpc, &keypair, &bls)
            .await
            .expect("ensure_registered");

        // Verify node now exists on chain
        let node = rpc.get_node(&keypair.address()).await.expect("get node");
        assert_eq!(
            node.metadata.bls_pubkey,
            bls.public_key().expect("bls pubkey")
        );
    }

    #[tokio::test]
    async fn skips_existing_node() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let bls = BlsPrivateKey::from_random();
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address).await;

        // Second call should succeed without registering again
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        ensure_registered(&config, &rpc, &keypair, &bls)
            .await
            .expect("ensure_registered idempotent");
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
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls_original, address).await;

        // Call with a different BLS keypair
        let bls_different = BlsPrivateKey::from_random();
        let config = test_config_with_address([10, 0, 0, 1], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let err = ensure_registered(&config, &rpc, &keypair, &bls_different)
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
        let address = NetworkAddress::from_socket_addr(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 443),
        );

        register_fresh_node(&harness, &keypair, &bls, address).await;

        // Config points to a different IP
        let config = test_config_with_address([10, 0, 0, 2], 443);
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let err = ensure_registered(&config, &rpc, &keypair, &bls)
            .await
            .unwrap_err();

        match err {
            NodeError::Config(msg) => assert!(msg.contains("network address")),
            other => panic!("expected Config error, got: {other}"),
        }
    }
}
