//! Verifies that peer-only routes reject unauthenticated and non-peer
//! clients, while public routes remain open.
//!
//! Exercises the full stack: `PeerIdentityAcceptor` extracts the client's
//! SPKI (or lack thereof) from the TLS session, `require_committee_peer`
//! middleware maps it to a known peer in `PeerManager` and checks committee
//! membership, and the handler is reached only when both checks pass.

use std::time::Duration;

use rand::thread_rng;
use reqwest::StatusCode;
use tape_core::types::BasisPoints;
use tape_crypto::p256::Keypair as P256Keypair;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

const NODE_COUNT: usize = 5;

#[tokio::test]
async fn peer_only_routes_reject_non_peers() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(false)
        .build()
        .expect("build harness");

    harness
        .bootstrap_nodes(BasisPoints(100), 1_000, Duration::from_secs(30))
        .await
        .expect("bootstrap nodes");

    let scenario = harness.scenario();
    scenario
        .wait_nodes_active(&(0..NODE_COUNT).collect::<Vec<_>>(), Duration::from_secs(20))
        .await
        .expect("all nodes active");

    let target = harness
        .nodes()
        .first()
        .expect("at least one node");
    let source = harness
        .nodes()
        .get(1)
        .expect("at least two nodes");
    let base = target.base_url();
    let pin = target.tls_pubkey();

    // --- Anonymous client: no client cert presented ---
    let anon = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        let builder = peer_tls::apply_pinned_tls(builder, pin).expect("anon tls");
        builder.build().expect("anon build")
    };

    let health = anon
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .expect("anon health");

    assert_eq!(
        health.status(),
        StatusCode::OK,
        "anonymous client should reach public /v1/health"
    );

    let stats = anon
        .get(format!("{base}/v1/stats"))
        .send()
        .await
        .expect("anon stats");

    assert_eq!(
        stats.status(),
        StatusCode::FORBIDDEN,
        "anonymous client must be rejected from peer-only /v1/stats"
    );

    let vote = anon
        .post(format!("{base}/v1/snapshots/vote"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("anon snapshot_vote");

    assert_eq!(
        vote.status(),
        StatusCode::FORBIDDEN,
        "anonymous client must be rejected from peer-only /v1/snapshots/vote"
    );

    // Impostor: valid P-256 client cert, but key is not on-chain
    let impostor_key = P256Keypair::generate(&mut thread_rng());
    let impostor = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        let builder = peer_tls::apply_pinned_tls_with_identity(builder, pin, &impostor_key)
            .expect("impostor tls");
        builder.build().expect("impostor build")
    };

    let impostor_vote = impostor
        .post(format!("{base}/v1/snapshots/vote"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("impostor snapshot_vote");

    assert_eq!(
        impostor_vote.status(),
        StatusCode::FORBIDDEN,
        "client with unregistered TLS key must be rejected from peer-only routes"
    );

    // Sanity: impostor can still hit public routes.
    let impostor_health = impostor
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .expect("impostor health");

    assert_eq!(
        impostor_health.status(),
        StatusCode::OK,
        "impostor should still reach public /v1/health"
    );

    // --- Real peer: registered TLS key and current committee membership ---
    let peer = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        let builder = peer_tls::apply_pinned_tls_with_identity(builder, pin, source.tls_keypair())
            .expect("peer tls");
        builder.build().expect("peer build")
    };

    let peer_stats = peer
        .get(format!("{base}/v1/stats"))
        .send()
        .await
        .expect("peer stats");
    assert_eq!(
        peer_stats.status(),
        StatusCode::OK,
        "registered committee peer should reach peer-only /v1/stats"
    );

    harness.stop_all().await.expect("stop runtimes");
}
