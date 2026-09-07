//! Verifies that peer-only routes reject unauthenticated and non-peer
//! clients, while public routes remain open.

use std::time::Duration;

use rand::thread_rng;
use reqwest::StatusCode;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, GroupIndex, RoundNumber};
use tape_crypto::ed25519::Keypair as EdKeypair;
use tape_crypto::hash::Hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_node::features::vote::member_groups;
use tape_protocol::api::{AttestationPayload, CHALLENGE_ATTEST_PATH, SpoolAttestation, VOTE_PATH};

const NODE_COUNT: usize = GROUP_SIZE;

#[test]
fn peer_only_routes_reject_non_peers() {
    run_simnet_test(peer_only_routes_reject_non_peers_inner);
}

async fn peer_only_routes_reject_non_peers_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(false)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(20))
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
        StatusCode::OK,
        "anonymous client should reach public /v1/stats"
    );

    let vote = anon
        .post(format!("{base}{VOTE_PATH}"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("anon vote");

    assert_eq!(
        vote.status(),
        StatusCode::FORBIDDEN,
        "anonymous client must be rejected from peer-only {VOTE_PATH}"
    );

    let attest = anon
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("anon attest");

    assert_eq!(
        attest.status(),
        StatusCode::FORBIDDEN,
        "anonymous client must be rejected from peer-only {CHALLENGE_ATTEST_PATH}"
    );

    // Impostor: valid Ed25519 client cert, but key is not on-chain
    let impostor_key = EdKeypair::new(&mut thread_rng());
    let impostor = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        let builder = peer_tls::apply_pinned_tls_with_identity(builder, pin, &impostor_key)
            .expect("impostor tls");
        builder.build().expect("impostor build")
    };

    let impostor_vote = impostor
        .post(format!("{base}{VOTE_PATH}"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("impostor vote");

    assert_eq!(
        impostor_vote.status(),
        StatusCode::FORBIDDEN,
        "client with unregistered TLS key must be rejected from peer-only routes"
    );

    let impostor_attest = impostor
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("impostor attest");

    assert_eq!(
        impostor_attest.status(),
        StatusCode::FORBIDDEN,
        "client with unregistered TLS key must be rejected from {CHALLENGE_ATTEST_PATH}"
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
        "registered committee peer should reach public /v1/stats"
    );

    let peer_vote = peer
        .post(format!("{base}{VOTE_PATH}"))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("peer vote");
    assert_eq!(
        peer_vote.status(),
        StatusCode::BAD_REQUEST,
        "registered committee peer should pass auth and fail only on malformed vote body"
    );

    // Nothing verifies an attestation on arrival, so a peer naming somebody else
    // as the signer would seat a signature that spoils the round's aggregate.
    let source_address = source.context().node_address();
    let state = source.context().state();
    let held = member_groups(&state.member_spools(source_address));
    let mine = *held.first().expect("source holds a spool");
    // A group the source has no position in, or one past the roster if it
    // holds a position in every group.
    let foreign = (0..=state.current.groups.len() as u64)
        .map(GroupIndex)
        .find(|group| !held.contains(group))
        .expect("a group outside the source");

    // The roster is the round's epoch's, so the payload names the epoch the
    // node is in.
    let forged = AttestationPayload {
        epoch: state.epoch(),
        group: mine,
        round: RoundNumber(0),
        block: Hash([0; 32]),
        signer: target.context().node_address(),
        attests: Vec::new(),
    };
    let peer_attest = peer
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(wincode::serialize(&forged).expect("serialize attestation"))
        .send()
        .await
        .expect("peer attest");
    assert_eq!(
        peer_attest.status(),
        StatusCode::FORBIDDEN,
        "a peer must not post an attestation it claims another node signed"
    );

    // A signer outside the group would fill a quorum position its roster
    // never had.
    let outsider = AttestationPayload {
        signer: source_address,
        group: foreign,
        ..forged.clone()
    };
    let peer_outsider_attest = peer
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(wincode::serialize(&outsider).expect("serialize attestation"))
        .send()
        .await
        .expect("peer outsider attest");
    assert_eq!(
        peer_outsider_attest.status(),
        StatusCode::FORBIDDEN,
        "a peer must not attest for a group it holds no spool in"
    );

    // A spool's group is fixed by its index, so a batch naming another group's
    // spool is malformed.
    let stray = AttestationPayload {
        signer: source_address,
        attests: vec![SpoolAttestation {
            spool: foreign.base_spool(),
            signature: source.bls_keypair().sign(b"stray").expect("sign"),
        }],
        ..forged.clone()
    };
    let peer_stray_attest = peer
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(wincode::serialize(&stray).expect("serialize attestation"))
        .send()
        .await
        .expect("peer stray attest");
    assert_eq!(
        peer_stray_attest.status(),
        StatusCode::BAD_REQUEST,
        "a batch must not carry a spool outside the group it names"
    );

    let own = AttestationPayload {
        signer: source_address,
        ..forged
    };
    let peer_own_attest = peer
        .post(format!("{base}{CHALLENGE_ATTEST_PATH}"))
        .body(wincode::serialize(&own).expect("serialize attestation"))
        .send()
        .await
        .expect("peer own attest");
    assert_eq!(
        peer_own_attest.status(),
        StatusCode::OK,
        "a peer signing under its own identity for its own group should pass auth"
    );

    harness.stop_all().await.expect("stop runtimes");
}
