use std::sync::Arc;
use std::time::{Duration, Instant};

use tape_peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::share::{ShareToken, ALL_TRACKS};
use tape_core::track::types::TrackVisibility;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::{hash, Hash};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetScenario, TAPENET_GROUP_SIZE, run_simnet_test,
};
use tape_protocol::api::FindTrackVersion;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::read_options::ReadOptions;
use tape_sdk::tapedrive::Tapedrive;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
const SHARE_TTL: Duration = Duration::from_secs(3600);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);

type Client = Tapedrive<LiteSvmRpc, HttpApi>;

// hidden coded and inline objects serve only to readers holding a share token
#[test]
fn hidden_tracks() {
    run_simnet_test(hidden_tracks_inner);
}

async fn hidden_tracks_inner() {
    let node_count = TAPENET_GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

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

    let owner = scenario.sdk(harness.admin());
    let bucket = TapeKey::generate();
    owner
        .reserve(&bucket, StorageUnits::mb(4), 4)
        .await
        .expect("reserve bucket tape");

    let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
        visibility: TrackVisibility::Hidden,
        ..WriteOptions::default()
    });

    let name = "hidden/notes.txt";
    let data = vec![0x5Au8; 64 * 1024];
    writer
        .put_object(&bucket, name, &data, Some("text/plain"))
        .await
        .expect("hidden coded write");
    let tiny_name = "hidden/tiny.txt";
    let tiny = b"a short note".to_vec();
    writer
        .put_object(&bucket, tiny_name, &tiny, Some("text/plain"))
        .await
        .expect("hidden inline write");
    let key = hash(name.as_bytes());
    wait_track_visible(&owner, &bucket.address(), key).await;
    wait_track_visible(&owner, &bucket.address(), hash(tiny_name.as_bytes())).await;

    // a caller with no mTLS identity and no share token cannot fetch either object
    assert!(owner.get_object(&bucket.address(), name).await.is_err());
    assert!(owner.get_object(&bucket.address(), tiny_name).await.is_err());

    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let token = owner
        .share(&bucket, key, reader_tls, SHARE_TTL)
        .await
        .expect("share one object");
    let reader = reader_client(&scenario, reader_identity.clone(), token);
    assert_eq!(
        reader.get_object(&bucket.address(), name).await.expect("shared read"),
        data,
        "hidden coded object should round-trip"
    );

    let tape_token = owner
        .share(&bucket, ALL_TRACKS, reader_tls, SHARE_TTL)
        .await
        .expect("share every object");
    let tape_reader = reader_client(&scenario, reader_identity.clone(), tape_token);
    assert_eq!(
        tape_reader.get_object(&bucket.address(), tiny_name).await.expect("all-tracks read"),
        tiny,
        "hidden inline object should round-trip"
    );

    // the same token presented by another identity is refused
    let impostor = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let borrowed = reader_client(&scenario, impostor, token);
    assert!(borrowed.get_object(&bucket.address(), name).await.is_err());

    let expired = owner
        .share(&bucket, key, reader_tls, Duration::from_secs(0))
        .await
        .expect("share with no lifetime");
    let stale = reader_client(&scenario, reader_identity.clone(), expired);
    assert!(stale.get_object(&bucket.address(), name).await.is_err());

    // a put-over keeps the name, so the token for that key opens the new version
    let previous = track_number(&owner, &bucket.address(), key).await;
    let second = vec![0xA5u8; 32 * 1024];
    writer
        .put_object(&bucket, name, &second, Some("text/plain"))
        .await
        .expect("put over the hidden object");
    // reads go to whichever node answers first, so every node must hold the put-over
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the put-over");
    wait_track_advanced(&owner, &bucket.address(), key, previous).await;
    assert_eq!(
        reader.get_object(&bucket.address(), name).await.expect("read the put-over"),
        second,
        "the put-over should round-trip"
    );

    owner
        .revoke_shared_tokens(&bucket)
        .await
        .expect("revoke shared tokens");
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the share nonce advance");
    assert!(reader.get_object(&bucket.address(), name).await.is_err());
    assert!(tape_reader.get_object(&bucket.address(), tiny_name).await.is_err());

    harness.stop_all().await.expect("stop runtimes");
}

fn reader_client(
    scenario: &SimnetScenario<'_>,
    identity: Arc<CryptoKeypair>,
    token: ShareToken,
) -> Client {
    scenario
        .reader_sdk(identity)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            share_token: Some(token),
            ..ReadOptions::default()
        })
}

/// A write can confirm before every node replays its block, so reads by name wait for the track
async fn wait_track_visible(client: &Client, tape: &Address, key: Hash) {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    while client.find_track(tape, key, FindTrackVersion::Latest).await.is_err() {
        assert!(Instant::now() < deadline, "track was not visible on nodes within {VISIBILITY_TIMEOUT:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Ask the nodes for a name's latest version
async fn track_number(client: &Client, tape: &Address, key: Hash) -> TrackNumber {
    client
        .find_track(tape, key, FindTrackVersion::Latest)
        .await
        .expect("track visible before the put-over")
        .track_number
}

/// Wait until the answering node's latest version for a name moves past `previous`
async fn wait_track_advanced(client: &Client, tape: &Address, key: Hash, previous: TrackNumber) {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    loop {
        if let Ok(track) = client.find_track(tape, key, FindTrackVersion::Latest).await {
            if track.track_number > previous {
                return;
            }
        }
        assert!(Instant::now() < deadline, "nodes still report {previous:?} after {VISIBILITY_TIMEOUT:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
