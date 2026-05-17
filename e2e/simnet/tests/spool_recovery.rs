use std::time::{Duration, Instant};

use rand::Rng as _;
use tape_api::program::EPOCH_DURATION;
use tape_core::erasure::{SPOOL_COUNT, GROUP_SIZE};
use tape_core::types::BasisPoints;
use tape_crypto::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};
use tape_core::system::SpoolStatus;

/// Full spool recovery flow: upload blob, drop nodes, verify
/// Sync/Scan/Repair/Recover workers converge back to Active, then download.
#[test]
fn spool_recovery() {
    let thread = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(spool_recovery_inner())
        })
        .expect("spawn test thread");

    thread.join().unwrap();
}

async fn spool_recovery_inner() {
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
    let timeout = Duration::from_secs(60);
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

    // Upload a small blob
    let key = hash::hash(b"spool-recovery-test");
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

    // Verify all slices are stored
    let spool_group = track.spool_group;
    let slice_count = scenario
        .count_slices(&track_address, spool_group)
        .expect("count slices");
    assert_eq!(slice_count, GROUP_SIZE);

    // Crash 5 random nodes (previous spool owners will be unreachable)
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

    // Advance epoch 3 → 4 (surviving nodes get reassigned spools and begin Sync).
    let scenario = harness.scenario();
    let epoch4 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 4");
    assert_eq!(epoch4, 4);

    scenario
        .wait_nodes_active(&alive_indices, timeout)
        .await
        .expect("alive nodes active at epoch 4");

    // Advance epoch 4 → 5 so dead nodes drop out of committee
    // (they joined during epoch 3 so they're still in epoch 4's committee)
    let epoch5 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 5");
    assert_eq!(epoch5, 5);

    scenario
        .wait_nodes_active(&alive_indices, timeout)
        .await
        .expect("alive nodes active at epoch 5");

    // Wait for spool reconciliation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all spools are assigned to alive nodes
    let alive_total = scenario
        .total_spool_count(&alive_indices)
        .expect("total spool count");
    assert_eq!(alive_total, SPOOL_COUNT);

    // Poll until any Sync spools transition into later phases.
    let sync_timeout = Duration::from_secs(120);
    let start = Instant::now();
    loop {
        let mut any_sync = false;
        for &i in &alive_indices {
            let statuses = scenario.node_spool_statuses(i).expect("spool statuses");
            for (_, state) in &statuses {
                if matches!(state.status, SpoolStatus::Sync) {
                    any_sync = true;
                    break;
                }
            }
            if any_sync {
                break;
            }
        }

        if !any_sync {
            break;
        }

        assert!(
            start.elapsed() < sync_timeout,
            "timed out waiting for Sync spools to transition"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Poll until all spools reach terminal states again.
    let active_timeout = Duration::from_secs(120);
    let start = Instant::now();
    loop {
        let mut all_active = true;
        for &i in &alive_indices {
            let statuses = scenario.node_spool_statuses(i).expect("spool statuses");
            for (_, state) in &statuses {
                if !matches!(state.status, SpoolStatus::Active | SpoolStatus::LockedToMove) {
                    all_active = false;
                    break;
                }
            }
            if !all_active {
                break;
            }
        }

        if all_active {
            break;
        }

        assert!(
            start.elapsed() < active_timeout,
            "timed out waiting for all spools to reach Active"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Download the blob and verify data integrity
    let downloaded = scenario
        .download(harness.admin(), &track_address)
        .await
        .expect("download blob after recovery");
    assert_eq!(downloaded, data, "downloaded data should match original");

    harness.stop_all().await.expect("stop all");
}
