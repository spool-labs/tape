use std::time::Duration;

use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::BasisPoints;
use tape_crypto::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

#[tokio::test]
async fn blob_upload() {
    let node_count = 25;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    harness
        .bootstrap_nodes(0, BasisPoints(100), 1_000, health_timeout)
        .await
        .expect("bootstrap nodes");

    let all: Vec<usize> = (0..node_count).collect();
    let timeout = Duration::from_secs(30);

    let scenario = harness.scenario();
    scenario
        .wait_nodes_active(&all, timeout)
        .await
        .expect("all nodes active");

    // Advance to epoch 2 then 3 so committee is fully active
    let epoch2 = scenario
        .self_advance_epoch(timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch2, 2);

    let epoch3 = scenario
        .self_advance_epoch(timeout)
        .await
        .expect("advance to epoch 3");
    assert_eq!(epoch3, 3);

    // Upload a 10 KB blob
    let key = hash::hash(b"e2e-test-blob");
    let data: Vec<u8> = (0..10_240).map(|i| (i % 256) as u8).collect();

    let (tape_key, track) = scenario
        .upload(0, key, &data, 4)
        .await
        .expect("upload blob");

    // Verify track was certified with a non-zero commitment
    let zero = [0u8; 32];
    assert_ne!(
        track.data.commitment_hash.as_ref(),
        &zero,
        "commitment hash should be non-zero"
    );

    // Verify slices stored across the spool group
    let spool_group = track.data.spool_group();
    let track_address = tape_key.track_address(&key);
    let slice_count = scenario
        .count_slices(&track_address, spool_group)
        .expect("count slices");
    assert_eq!(
        slice_count, SPOOL_GROUP_SIZE,
        "expected {SPOOL_GROUP_SIZE} slices stored, got {slice_count}"
    );

    // Download and verify data integrity
    let downloaded = scenario
        .download(0, &track_address)
        .await
        .expect("download blob");
    assert_eq!(downloaded, data, "downloaded data should match original");

    harness.stop_all().await.expect("stop all");
}
