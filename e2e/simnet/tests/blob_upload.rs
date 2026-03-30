use std::time::Duration;

use tape_api::program::EPOCH_DURATION;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::BasisPoints;
use tape_crypto::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

#[test]
fn blob_upload() {
    // The SDK write/read futures are large; run on a thread with extra stack.
    let thread = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(blob_upload_inner())
        })
        .expect("spawn test thread");

    thread.join().unwrap();
}

async fn blob_upload_inner() {
    let node_count = 25;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    harness
        .bootstrap_nodes(BasisPoints(100), 1_000, health_timeout)
        .await
        .expect("bootstrap nodes");

    let all: Vec<usize> = (0..node_count).collect();
    let timeout = Duration::from_secs(30);
    let epoch_timeout = Duration::from_secs(EPOCH_DURATION as u64 * 2);

    let scenario = harness.scenario();
    scenario
        .wait_nodes_active(&all, timeout)
        .await
        .expect("all nodes active");

    // Advance to epoch 2 then 3 so committee is fully active
    let epoch2 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch2, 2);

    let epoch3 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 3");
    assert_eq!(epoch3, 3);

    // Upload a 10 KB blob
    let key = hash::hash(b"e2e-test-blob");
    let data: Vec<u8> = (0..10_240).map(|i| (i % 256) as u8).collect();

    let (_tape_key, track_address, track) = scenario
        .upload(harness.admin(), key, &data, 4)
        .await
        .expect("upload blob");

    assert!(track.is_blob(), "uploaded track should be a blob track");
    assert!(
        track.is_certified(),
        "uploaded blob track should be certified"
    );

    // Verify slices stored across the spool group
    let spool_group = track.spool_group;
    let slice_count = scenario
        .count_slices(&track_address, spool_group)
        .expect("count slices");
    assert_eq!(
        slice_count, SPOOL_GROUP_SIZE,
        "expected {SPOOL_GROUP_SIZE} slices stored, got {slice_count}"
    );

    // Download and verify data integrity
    let downloaded = scenario
        .download(harness.admin(), &track_address)
        .await
        .expect("download blob");
    assert_eq!(downloaded, data, "downloaded data should match original");

    harness.stop_all().await.expect("stop all");
}
