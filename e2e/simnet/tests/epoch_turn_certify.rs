use std::sync::Arc;
use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use rpc_litesvm::LiteSvmRpc;
use tape_core::prelude::CompressedTrack;
use tape_core::types::{BasisPoints, ContentType, StorageUnits};
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, run_simnet_test};
use tape_peer_http::HttpApi;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::metrics::{InMemory, Operation, Phase};
use tape_sdk::object::ObjectBatchItem;
use tape_sdk::tapedrive::Tapedrive;

const TARGET_GROUPS: u64 = 1;

/// Coded objects in the batch; each one is certified on its own leaf.
const BATCH_ITEMS: usize = 4;

#[test]
fn epoch_turn_certify() {
    run_simnet_test(epoch_turn_certify_inner);
}

/// A batch stored under one epoch and certified after the chain turned signs
/// again and lands, without a reseed and without a peer proof.
async fn epoch_turn_certify_inner() {
    let node_count = TAPENET_GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

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

    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");
    let stored_epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");

    let metrics = Arc::new(InMemory::new());
    let sdk = scenario.sdk(harness.admin()).with_metrics(metrics.clone());
    let bucket = TapeKey::generate();

    sdk.reserve(&bucket, StorageUnits::mb(1), 4)
        .await
        .expect("reserve bucket tape");

    let objects: Vec<ObjectBatchItem> = (0..BATCH_ITEMS)
        .map(|index| ObjectBatchItem {
            name: format!("sheet-{index}.css"),
            data: vec![b'a' + index as u8; 4 * 1024],
            content_type: ContentType::TextCss,
            plan: None,
        })
        .collect();

    let stored = sdk
        .store_objects_batch(&bucket, objects)
        .await
        .expect("store the batch under the old epoch");
    assert_eq!(stored.receipts.len(), BATCH_ITEMS, "every object gets a receipt");

    // The receipts the upload banked, and the committee the client holds, both
    // belong to this epoch.
    let certified_epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("turn the epoch under the stored batch");
    assert_eq!(
        certified_epoch,
        stored_epoch + 1,
        "the batch was stored one epoch before it certifies"
    );
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active after the turn");

    // Waiting out the turn ages the cached committee past its trust window,
    // which would refresh it before the first certify and hide the refusal.
    // A writer mid-batch when the chain turns still holds a fresh one.
    sdk.state().touch();
    metrics.clear();

    sdk.certify_objects_batch(&bucket, stored.verification)
        .await
        .expect("certify the batch across the epoch turn");

    let submits = batch_phase_count(&metrics, Phase::CertifySubmit);
    let collects = batch_phase_count(&metrics, Phase::CertifyCollect);
    let proofs = batch_phase_count(&metrics, Phase::CertifyProof);

    assert_eq!(
        submits, 2,
        "one run refused on the old epoch and one that landed, nothing single-file"
    );
    assert_eq!(
        collects,
        (BATCH_ITEMS * 2) as u64,
        "every entry signs once under each epoch"
    );
    assert_eq!(proofs, 0, "the mirror keeps proving, so no peer proof is fetched");

    for receipt in &stored.receipts {
        let track = wait_for_certified_track(&sdk, &receipt.address, Duration::from_secs(10)).await;
        assert!(
            track.is_certified(),
            "{} should be certified after the turn",
            receipt.name
        );
    }

    harness.stop_all().await.expect("stop runtimes");
}

fn batch_phase_count(metrics: &InMemory, phase: Phase) -> u64 {
    let mut count = 0;
    for (key, summary) in metrics.snapshot() {
        if key.operation == Operation::WriteBatch && key.phase == phase {
            count += summary.count;
        }
    }
    count
}

async fn wait_for_certified_track(
    sdk: &Tapedrive<LiteSvmRpc, HttpApi>,
    track: &Address,
    timeout: Duration,
) -> CompressedTrack {
    let start = Instant::now();
    loop {
        let current = sdk.get_track(track).await.expect("fetch batch track");
        if current.is_certified() || start.elapsed() >= timeout {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
