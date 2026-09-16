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
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);

#[test]
fn restricted_track_flow() {
    run_simnet_test(restricted_track_flow_inner);
}

async fn restricted_track_flow_inner() {
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
        visibility: TrackVisibility::Restricted,
        ..WriteOptions::default()
    });

    let name = "restricted/notes.txt";
    let data = vec![0x5Au8; 64 * 1024];
    writer
        .put_object(&bucket, name, &data, Some("text/plain"))
        .await
        .expect("restricted write registers and certifies");
    let payload_key = hash(name.as_bytes());
    wait_track_visible(&owner, &bucket.address(), payload_key, Duration::from_secs(60)).await;

    // A caller with no mTLS identity and no grant cannot fetch the bytes at all.
    let refused = owner.get_object(&bucket.address(), name).await;
    assert!(refused.is_err(), "ungranted read should be refused");

    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let grant = owner
        .issue_read_grant(&bucket, payload_key, reader_tls, GRANT_TTL)
        .await
        .expect("issue read grant");

    // The grant plus the identity it names is the whole story.
    let reader = scenario
        .reader_sdk(reader_identity.clone())
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });
    let opened = reader
        .get_object(&bucket.address(), name)
        .await
        .expect("granted read succeeds");
    assert_eq!(opened, data, "restricted object should round-trip");

    // The same grant presented by an identity it does not name is refused.
    let impostor = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let borrowed = scenario
        .reader_sdk(impostor)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });
    assert!(
        borrowed.get_object(&bucket.address(), name).await.is_err(),
        "a grant bound to another reader should be refused"
    );

    // An expired grant is refused.
    let expired = owner
        .issue_read_grant(&bucket, payload_key, reader_tls, Duration::from_secs(0))
        .await
        .expect("issue expired grant");
    let stale = scenario
        .reader_sdk(reader_identity.clone())
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(expired),
            ..ReadOptions::default()
        });
    assert!(
        stale.get_object(&bucket.address(), name).await.is_err(),
        "an expired grant should be refused"
    );

    // A put-over keeps the name, so the grant on that key still opens the new version.
    let payload_v1 = track_number(&owner, &bucket.address(), payload_key).await;
    let second = vec![0xA5u8; 32 * 1024];
    writer
        .put_object(&bucket, name, &second, Some("text/plain"))
        .await
        .expect("put over the restricted object");

    // Reads are answered by whichever node replies first, so every node has to
    // hold the put-over before one can be trusted to serve it.
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the put-over");
    wait_track_advanced(&owner, &bucket.address(), payload_key, payload_v1, VISIBILITY_TIMEOUT).await;

    let reopened = reader
        .get_object(&bucket.address(), name)
        .await
        .expect("read the put-over");
    assert_eq!(reopened, second, "the put-over should round-trip");

    // Bumping the generation revokes every grant outstanding on the tape.
    owner
        .bump_grant_generation(&bucket)
        .await
        .expect("bump grant generation");
    // The bump is a chain transaction; nodes enforce it once they ingest it.
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the generation bump");
    assert!(
        reader.get_object(&bucket.address(), name).await.is_err(),
        "a generation bump should revoke outstanding grants"
    );

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

/// Latest version the nodes report for a name.
async fn track_number(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
) -> tape_core::types::TrackNumber {
    client
        .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
        .await
        .expect("track visible before the put-over")
        .track_number
}

/// A put-over is visible when the answering node's latest pointer has moved
/// past the version it held before.
async fn wait_track_advanced(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
    previous: tape_core::types::TrackNumber,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(track) = client
            .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
            .await
        {
            if track.track_number > previous {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nodes still report {previous:?} for the put-over after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
