use std::sync::Arc;
use std::time::Duration;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::TrackVisibility;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::read_options::ReadOptions;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
const GRANT_TTL: Duration = Duration::from_secs(3600);

/// An inline payload rides the transaction, so a restricted one is gated on the
/// node's read surface exactly like a coded one.
#[test]
fn restricted_inline_round_trip() {
    run_simnet_test(restricted_inline_round_trip_inner);
}

async fn restricted_inline_round_trip_inner() {
    let node_count = GROUP_SIZE;
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
    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");

    let owner = scenario.sdk(harness.admin());
    let bucket = TapeKey::generate();
    owner
        .reserve(&bucket, StorageUnits::mb(1), 4)
        .await
        .expect("reserve bucket tape");

    let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
        visibility: TrackVisibility::Restricted,
        ..WriteOptions::default()
    });

    let name = "restricted/tiny.txt";
    let data = b"a short note";
    writer
        .put_object(&bucket, name, data, Some("text/plain"))
        .await
        .expect("restricted inline write");
    wait_track_visible(&owner, &bucket.address(), hash(name.as_bytes()), Duration::from_secs(60)).await;

    assert!(
        owner.get_object(&bucket.address(), name).await.is_err(),
        "an ungranted read of a restricted inline object should be refused"
    );

    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let grant = owner
        .issue_read_grant(&bucket, hash(name.as_bytes()), reader_tls, GRANT_TTL)
        .await
        .expect("issue read grant");

    let reader = scenario
        .reader_sdk(reader_identity)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });
    let opened = reader
        .get_object(&bucket.address(), name)
        .await
        .expect("read the restricted inline object");
    assert_eq!(opened, data, "restricted inline should round-trip");

    harness.stop_all().await.expect("stop runtimes");
}

/// A write returns once slices are held and certification confirms, which can be
/// before every node has replayed the registration block; reads by name need the
/// nodes to have caught up, so tests wait for the track to become findable.
async fn wait_track_visible(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if client
            .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "track was not visible on nodes within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
