use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, ContentType, StorageUnits};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::object::{ListObjectsQuery, ObjectBatchItem};

const TARGET_GROUPS: u64 = 1;

#[test]
fn object_layer_round_trip() {
    run_simnet_test(object_layer_round_trip_inner);
}

async fn object_layer_round_trip_inner() {
    let node_count = GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    let health_timeout = Duration::from_secs(30);

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
        .wait_nodes_healthy(health_timeout)
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

    let sdk = scenario.sdk(harness.admin());
    let bucket = TapeKey::generate();
    let name = "photos/cat.jpg";
    let data = b"named object bytes";

    sdk.reserve(&bucket, StorageUnits::mb(1), 4)
        .await
        .expect("reserve bucket tape");

    sdk.put_object(&bucket, name, data, Some("image/jpeg"))
        .await
        .expect("put object");

    let listed = wait_for_objects(&sdk, &bucket.address(), "photos/", &[name], Duration::from_secs(10))
        .await;
    assert_eq!(listed, vec![name.to_string()], "object list mismatch");

    let read = sdk
        .get_object(&bucket.address(), name)
        .await
        .expect("get object");
    assert_eq!(read, data, "object bytes should round-trip");

    sdk.delete_object(&bucket, name).await.expect("delete object");

    let listed = wait_for_objects(&sdk, &bucket.address(), "photos/", &[], Duration::from_secs(10))
        .await;
    assert!(listed.is_empty(), "object list should be empty after delete");

    // A mixed inline/coded batch retains caller order, certifies every coded
    // track, and leaves all objects independently readable. The entry point
    // is deliberately last, matching the site publication contract.
    let script = b"console.log('batch');".to_vec();
    let stylesheet = vec![b'x'; 4 * 1024];
    let index = b"<html><script src=\"app.js\"></script></html>".to_vec();
    let batch = vec![
        ObjectBatchItem {
            name: "app.js".into(),
            data: script.clone(),
            content_type: ContentType::TextJavascript,
            plan: None,
        },
        ObjectBatchItem {
            name: "style.css".into(),
            data: stylesheet.clone(),
            content_type: ContentType::TextCss,
            plan: None,
        },
        ObjectBatchItem {
            name: "index.html".into(),
            data: index.clone(),
            content_type: ContentType::TextHtml,
            plan: None,
        },
    ];
    let stored = sdk
        .store_objects_batch(&bucket, batch)
        .await
        .expect("store ordered object batch");
    let receipts = stored.receipts;
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.name.as_str())
            .collect::<Vec<_>>(),
        vec!["app.js", "style.css", "index.html"],
        "batch receipts must retain caller order"
    );
    let stylesheet_track = sdk
        .get_track(&receipts[1].address)
        .await
        .expect("fetch stored coded stylesheet");
    assert!(
        !stylesheet_track.is_certified(),
        "coded batch object should remain pending at the optimistic boundary"
    );
    sdk.certify_objects_batch(&bucket, stored.verification)
        .await
        .expect("certify stored object batch");
    let stylesheet_track = wait_for_certified_track(
        &sdk,
        &receipts[1].address,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        stylesheet_track.is_certified(),
        "coded batch object should certify through its verification handle"
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "app.js")
            .await
            .expect("read batch script"),
        script
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "style.css")
            .await
            .expect("read coded batch stylesheet"),
        stylesheet
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "index.html")
            .await
            .expect("read batch entry point"),
        index
    );

    let fully_put = sdk
        .put_objects_batch(
            &bucket,
            vec![ObjectBatchItem {
                name: "complete.css".into(),
                data: vec![b'y'; 4 * 1024],
                content_type: ContentType::TextCss,
                plan: None,
            }],
        )
        .await
        .expect("put fully verified object batch");
    let complete_track = wait_for_certified_track(
        &sdk,
        &fully_put[0].address,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        complete_track.is_certified(),
        "put_objects_batch should retain its fully verified contract"
    );

    harness.stop_all().await.expect("stop runtimes");
}

async fn wait_for_certified_track(
    sdk: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    track: &tape_crypto::Address,
    timeout: Duration,
) -> tape_core::prelude::CompressedTrack {
    let start = Instant::now();
    loop {
        let current = sdk.get_track(track).await.expect("fetch batch track");
        if current.is_certified() || start.elapsed() >= timeout {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_objects(
    sdk: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    bucket: &tape_crypto::Address,
    prefix: &str,
    expected: &[&str],
    timeout: Duration,
) -> Vec<String> {
    let start = Instant::now();
    loop {
        let objects = sdk
            .list_objects(bucket, ListObjectsQuery::new(prefix))
            .await
            .expect("list objects");
        let objects = objects
            .objects
            .into_iter()
            .map(|object| {
                String::from_utf8(object.name).expect("object name should be utf-8")
            })
            .collect::<Vec<_>>();
        let expected: Vec<String> = expected.iter().map(|name| (*name).to_string()).collect();
        if objects == expected || start.elapsed() >= timeout {
            return objects;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
