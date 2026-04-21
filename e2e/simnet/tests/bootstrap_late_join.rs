use std::time::Duration;

use tape_api::program::EPOCH_DURATION;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};
use tape_store::ops::{MetaOps, TrackOps};

#[test]
fn bootstrap_late_join() {
    let thread = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(bootstrap_late_join_inner())
        })
        .expect("spawn test thread");

    thread.join().unwrap();
}

async fn bootstrap_late_join_inner() {
    const INITIAL_NODES: usize = 25;

    let initial_nodes: Vec<usize> = (0..INITIAL_NODES).collect();
    let mut harness = SimnetBuilder::new()
        .node_count(INITIAL_NODES)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(EPOCH_DURATION as u64 * 5);

    harness
        .bootstrap_nodes(BasisPoints(100), 1_000, health_timeout)
        .await
        .expect("bootstrap initial nodes");

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_active(&initial_nodes, active_timeout)
            .await
            .expect("initial nodes active");

        while scenario
            .current_epoch_number()
            .await
            .expect("current epoch") < 5
        {
            scenario
                .self_advance_epoch(epoch_timeout)
                .await
                .expect("advance epoch");
        }
        scenario
            .wait_phase("Active", active_timeout)
            .await
            .expect("epoch 5 active");
    }

    let late_node = harness.add_node().expect("add late node");

    {
        let scenario = harness.scenario();
        scenario
            .register_many(&[late_node], BasisPoints(100))
            .await
            .expect("register late node");
        scenario
            .stake_many(&[late_node], 1_000)
            .await
            .expect("stake late node");
        scenario
            .pool_many(&[late_node])
            .await
            .expect("advance late pool");
        scenario
            .join_many(&[late_node])
            .await
            .expect("join late node");
    }

    harness
        .start_nodes_with_retry(&[late_node], 3, Duration::from_millis(200))
        .await
        .expect("start late node");

    {
        let scenario = harness.scenario();
        scenario
            .wait_node_healthy(late_node, health_timeout)
            .await
            .expect("late node healthy");
    }

    let late = harness.node(late_node).expect("late node exists");
    let ctx = late.context();
    let cursor = ctx
        .store
        .get_bootstrap_target_epoch()
        .expect("read bootstrap target")
        .expect("late node should have replayed snapshots");
    let current = ctx.state().epoch;
    let lower = EpochNumber(current.0.saturating_sub(2));
    let upper = EpochNumber(current.0.saturating_sub(1));

    assert!(
        cursor >= lower && cursor <= upper,
        "bootstrap cursor {cursor} should be the newest finalized snapshot for current epoch {current}"
    );

    let tracks = ctx
        .store
        .iter_tracks_from(None, 1)
        .expect("read late node tracks");
    assert!(
        !tracks.is_empty(),
        "late node should have replayed snapshot track metadata"
    );

    let _ = harness.stop_all().await;
}
