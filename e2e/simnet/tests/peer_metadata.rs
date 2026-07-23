//! Peer metadata propagates to storage nodes from ingested events alone:
//! registration, the metadata setters, and pool-advance stake stamping all
//! land in the peer cache without any getProgramAccounts refresh.

use std::time::Duration;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::coin::TAPE;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::BasisPoints;
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TestGateway, run_simnet_test};

const NODE_COUNT: usize = GROUP_SIZE;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;

const RENAMED: &str = "sim-gateway-renamed";

fn moved_address() -> NetworkAddress {
    NetworkAddress::new_ipv4([127, 0, 0, 1], 45_678)
}

#[test]
fn peer_metadata_propagates_from_events() {
    run_simnet_test(peer_metadata_propagates_from_events_inner);
}

async fn peer_metadata_propagates_from_events_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let propagate_timeout = Duration::from_secs(30);

    harness
        .bootstrap_nodes(BasisPoints(100), STORAGE_NODE_STAKE, health_timeout)
        .await
        .expect("bootstrap storage nodes");
    harness
        .scenario()
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("storage nodes active");

    // Registration: the gateway never starts a runtime, so nodes can only
    // learn it from the ingested RegisterNode event.
    let gateway_node = tape_api::program::tapedrive::node_pda(gateway.authority().into()).0;
    let original_tls = gateway.tls_pubkey();
    {
        let scenario = harness.scenario();
        scenario
            .register_gateway(&gateway, BasisPoints(100))
            .await
            .expect("register gateway");
    }
    wait_gateway(&harness, gateway_node, propagate_timeout, "registration", move |peer| {
        peer.tls_pubkey == original_tls && peer.stake == TAPE(0)
    })
    .await;

    // Metadata setters: each change must land in every node's cache from the
    // parsed instruction alone.
    {
        let scenario = harness.scenario();
        scenario
            .set_gateway_name(&gateway, RENAMED)
            .await
            .expect("set gateway name");
        scenario
            .set_gateway_network_address(&gateway, moved_address())
            .await
            .expect("set gateway network address");
    }
    wait_gateway(&harness, gateway_node, propagate_timeout, "metadata setters", |peer| {
        peer.network_address == moved_address() && peer.name.starts_with(RENAMED.as_bytes())
    })
    .await;

    let rotated_tls = NetworkTlsPubkey::new_unique();
    {
        let scenario = harness.scenario();
        scenario
            .set_gateway_network_tls(&gateway, rotated_tls)
            .await
            .expect("set gateway network tls");
    }
    wait_gateway(&harness, gateway_node, propagate_timeout, "tls rotation", move |peer| {
        peer.tls_pubkey == rotated_tls
    })
    .await;

    // Stake stamping: staking then advancing the pool must move the cached
    // stake via the PoolAdvanced event, with no tip refresh.
    {
        let scenario = harness.scenario();
        scenario
            .stake_gateway(&gateway, GATEWAY_STAKE)
            .await
            .expect("stake gateway");

        let epoch2 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 2");
        assert_eq!(epoch2, 2, "expected epoch 2");
        let epoch3 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 3");
        assert_eq!(epoch3, 3, "expected epoch 3");

        scenario
            .advance_gateway_pool_ok(&gateway)
            .await
            .expect("advance gateway pool");
    }
    wait_gateway(&harness, gateway_node, propagate_timeout, "pool advance stake", |peer| {
        peer.stake > TAPE(0)
    })
    .await;

    // The gateway never served a request, so every assertion above was fed by
    // ingested events; the metadata survives alongside the stamped stake.
    let peer = harness
        .node(0)
        .expect("node 0")
        .context()
        .peer_manager
        .get(gateway_node)
        .expect("gateway peer");
    assert_eq!(peer.network_address, moved_address());
    assert!(peer.name.starts_with(RENAMED.as_bytes()));
    assert_eq!(peer.tls_pubkey, rotated_tls);
}

/// Wait until every running node's cached entry for the gateway satisfies the
/// predicate.
async fn wait_gateway(
    harness: &tape_e2e_simnet::SimnetHarness,
    gateway_node: Address,
    timeout: Duration,
    what: &str,
    check: impl Fn(&peer_manager::PeerNode) -> bool + Copy,
) {
    harness
        .wait_peers(timeout, what, |peers| {
            peers.get(gateway_node).is_some_and(|peer| check(&peer))
        })
        .await
        .expect(what)
}
