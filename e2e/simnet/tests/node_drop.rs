use std::time::Duration;

use rand::Rng as _;
use tape_api::program::EPOCH_DURATION;
use tape_core::erasure::SPOOL_COUNT;
use tape_core::types::BasisPoints;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};
use tape_store::types::SpoolStatus;

#[tokio::test]
async fn spool_node_drop() {
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

    // Wait for all nodes active at epoch 1
    let scenario = harness.scenario();
    scenario
        .wait_nodes_active(&all, timeout)
        .await
        .expect("all nodes active");

    // Advance epoch 1 → 2
    let epoch2 = scenario.self_advance_epoch(epoch_timeout).await.expect("advance to epoch 2");
    assert_eq!(epoch2, 2, "expected epoch 2");

    // Record initial spool assignments for all nodes
    let initial_counts: Vec<usize> = all
        .iter()
        .map(|&i| scenario.node_spool_count(i).expect("spool count"))
        .collect();
    let initial_total: usize = initial_counts.iter().sum();
    assert_eq!(initial_total, SPOOL_COUNT, "all spools assigned at epoch 2");

    // Pick 5 random nodes to crash (keep 20 = MIN_COMMITTEE_SIZE)
    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> = (0..node_count).collect();
    let mut drop_indices = Vec::with_capacity(5);
    for _ in 0..5 {
        let pick = rng.gen_range(0..indices.len());
        drop_indices.push(indices.swap_remove(pick));
    }
    drop_indices.sort();
    let alive_indices = indices;

    drop(scenario);
    harness
        .kill_nodes(&drop_indices)
        .expect("kill dropped nodes");

    // Self-advance epoch 2 → 3 (only 20 running nodes participate)
    let scenario = harness.scenario();
    let epoch3 = scenario.self_advance_epoch(epoch_timeout).await.expect("advance to epoch 3");
    assert_eq!(epoch3, 3, "expected epoch 3");

    // Wait for remaining nodes to reach active at epoch 3
    scenario
        .wait_nodes_active(&alive_indices, timeout)
        .await
        .expect("alive nodes active at epoch 3");

    // Wait for nodes to process the epoch change and reconcile spools
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify spool redistribution: alive nodes have at least SPOOL_COUNT spools.
    // May overcount due to deferred GC: old owner keeps LockedToMove alongside
    // new owner's ActiveSync until cleanup runs 2 epochs later.
    let alive_total = scenario
        .total_spool_count(&alive_indices)
        .expect("total spool count");
    assert!(
        alive_total >= SPOOL_COUNT,
        "expected >= {SPOOL_COUNT} spools on alive nodes, got {alive_total}"
    );

    // Verify newly gained spools have ActiveSync status
    let mut sync_count = 0usize;
    for &i in &alive_indices {
        let statuses = scenario.node_spool_statuses(i).expect("spool statuses");
        for (spool_id, state) in &statuses {
            if matches!(state.status, SpoolStatus::ActiveSync) {
                sync_count += 1;
            }
            assert!(
                matches!(state.status, SpoolStatus::Active | SpoolStatus::ActiveSync | SpoolStatus::LockedToMove),
                "node {i} spool {spool_id} unexpected status {:?}", state.status
            );
        }
    }
    assert!(
        sync_count > 0,
        "expected some spools in ActiveSync from departed nodes"
    );

    harness.stop_all().await.expect("stop all");
}
