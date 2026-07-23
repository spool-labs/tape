//! Verifies the atlas observe endpoint end to end: only configured observers
//! can read it over mTLS, stored objects and peer transfers land in the feed,
//! loopback callers are filtered out, and the cursor resumes cleanly.

use std::time::{Duration, Instant};

use rand::thread_rng;
use reqwest::StatusCode;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::ed25519::Keypair as EdKeypair;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_observe_api::AtlasRecent;
use tape_protocol::api::{ListTracksByTapeReq, OBSERVE_ATLAS_PATH};
use tape_protocol::Api;
use tape_sdk::keys::tape_key::TapeKey;

const TARGET_GROUPS: u64 = 1;

#[test]
fn atlas_feed() {
    run_simnet_test(atlas_feed_inner);
}

async fn atlas_feed_inner() {
    peer_tls::install_default_provider();

    let node_count = GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let observer_key = EdKeypair::new(&mut thread_rng());
    let observer_b58 = observer_key.address().to_string();
    for node in harness.nodes_mut() {
        node.add_observer(observer_b58.clone());
    }

    let all: Vec<usize> = (0..node_count).collect();
    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(Duration::from_secs(30))
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("all nodes active");
    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch, 2, "expected epoch 2");

    // a named object write should surface in the atlas feed via the live tail
    let sdk = scenario.sdk(harness.admin());
    let bucket = TapeKey::generate();
    sdk.reserve(&bucket, StorageUnits::mb(1), 4)
        .await
        .expect("reserve bucket tape");
    sdk.put_object(&bucket, "photos/cat.jpg", b"named object bytes", Some("image/jpeg"))
        .await
        .expect("put object");

    // a peer call with a payload from node 1 to node 0 records a transfer
    let source = harness.node(1).expect("node 1");
    let target_address = harness.node(0).expect("node 0").context().node_address();
    source
        .context()
        .api
        .list_tracks_by_tape(
            target_address,
            &ListTracksByTapeReq { tape: bucket.address(), cursor: None, limit: 10 },
        )
        .await
        .expect("list tracks over the peer link");

    let base = source.base_url();
    let pin = source.tls_pubkey();
    let atlas_url = format!("{base}{OBSERVE_ATLAS_PATH}");

    // anonymous callers and unknown identities never see the feed
    let anon = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        peer_tls::apply_pinned_tls(builder, pin)
            .expect("anon tls")
            .build()
            .expect("anon build")
    };
    let anon_status = anon.get(&atlas_url).send().await.expect("anon atlas").status();
    assert_eq!(anon_status, StatusCode::FORBIDDEN, "anonymous caller must be rejected");

    let impostor_key = EdKeypair::new(&mut thread_rng());
    let impostor = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        peer_tls::apply_pinned_tls_with_identity(builder, pin, &impostor_key)
            .expect("impostor tls")
            .build()
            .expect("impostor build")
    };
    let impostor_status = impostor.get(&atlas_url).send().await.expect("impostor atlas").status();
    assert_eq!(impostor_status, StatusCode::FORBIDDEN, "unknown identity must be rejected");

    // even a real committee peer is not an observer
    let peer = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        peer_tls::apply_pinned_tls_with_identity(
            builder,
            pin,
            harness.node(2).expect("node 2").tls_keypair(),
        )
        .expect("peer tls")
        .build()
        .expect("peer build")
    };
    let peer_status = peer.get(&atlas_url).send().await.expect("peer atlas").status();
    assert_eq!(peer_status, StatusCode::FORBIDDEN, "committee peer without observer role must be rejected");

    // the configured observer reads the feed
    let observer = {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        peer_tls::apply_pinned_tls_with_identity(builder, pin, &observer_key)
            .expect("observer tls")
            .build()
            .expect("observer build")
    };
    let recent = wait_for_object(&observer, &atlas_url, Duration::from_secs(20)).await;

    assert!(recent.seq > 0, "feed should have advanced");
    let object = recent
        .objects
        .iter()
        .find(|object| object.kind == "image/jpeg")
        .expect("stored object should surface in the feed");
    assert!(object.size > 0, "object size should be recorded");
    assert!(
        !recent.transfers.is_empty(),
        "the explicit peer call should have recorded a transfer"
    );
    // simnet clients dial from loopback, which the ring filters out
    assert!(recent.ips.is_empty(), "loopback callers must not be recorded");

    // the cursor drains: nothing older than seq comes back
    let drained: AtlasRecent = observer
        .get(format!("{atlas_url}?after={}", recent.seq))
        .send()
        .await
        .expect("cursor read")
        .json()
        .await
        .expect("cursor body");
    assert!(
        drained.objects.is_empty() && drained.ips.is_empty(),
        "a caught-up cursor should return nothing old"
    );

    harness.stop_all().await.expect("stop runtimes");
}

/// Poll the observer endpoint until the stored object shows up
async fn wait_for_object(client: &reqwest::Client, url: &str, timeout: Duration) -> AtlasRecent {
    let start = Instant::now();
    loop {
        let response = client.get(url).send().await.expect("observer atlas");
        assert_eq!(response.status(), StatusCode::OK, "observer must be accepted");
        let recent: AtlasRecent = response.json().await.expect("atlas body");
        let has_object = recent.objects.iter().any(|object| object.kind == "image/jpeg");
        if has_object || start.elapsed() >= timeout {
            return recent;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
