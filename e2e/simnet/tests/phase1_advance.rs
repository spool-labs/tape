use std::time::{Duration, Instant};

use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};
use tape_store::types::NodeStatus;
use tracing::trace;

#[tokio::test]
async fn phase1_single_node_advance_loop() {
    let mut harness = SimnetBuilder::new()
        .node_count(1)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(25_000)
        .file_log(true)
        .slot_advance_per_tx(1)
        .build()
        .expect("build harness");

    {
        let scenario = harness.scenario();
        trace!(test = "phase1_single_node_advance", phase = "bootstrap_setup");
        scenario
            .init_system(0)
            .await
            .expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario
            .stake_many(0, &[0], 1_000)
            .await
            .expect("stake bootstrap node");
        scenario
            .pool_many(0, &[0])
            .await
            .expect("pool bootstrap node");
        scenario
            .join_many(0, &[0])
            .await
            .expect("join bootstrap node");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let timeout = Duration::from_secs(30);
    let mut expected_epoch = EpochNumber(1);
    {
        let scenario = harness.scenario();
        trace!(test = "phase1_single_node_advance", phase = "bootstrap_wait");
        scenario
            .wait_nodes_healthy(timeout)
            .await
            .expect("nodes healthy");
        expected_epoch = scenario.read_epoch().await.expect("read initial epoch").id;

        // Bootstrap advance: external harness caller drives epoch 1->2 so committee becomes discoverable.
        scenario.warp_epoch().expect("warp first epoch");
        scenario
            .advance_epoch_any()
            .await
            .expect("bootstrap advance");

        expected_epoch = EpochNumber(expected_epoch.as_u64() + 1);
        scenario
            .wait_epoch(expected_epoch.as_u64(), timeout)
            .await
            .expect("wait for bootstrap epoch");
        scenario
            .wait_for_nodes_epoch(&[0], Some(expected_epoch), timeout)
            .await
            .expect("node store epoch catches bootstrap");
        trace!(
            test = "phase1_single_node_advance",
            expected_epoch = expected_epoch.as_u64(),
            "bootstrap epoch observed by node"
        );
        let start = Instant::now();
        while start.elapsed() < timeout {
            if scenario.node_status(0) == Some(NodeStatus::Active) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(scenario.node_status(0), Some(NodeStatus::Active));
    }

    {
        let scenario = harness.scenario();
        // Phase-1 self-driven advances
        trace!(test = "phase1_single_node_advance", phase = "self_driven_advance_1");
        scenario.warp_epoch().expect("warp second epoch");
        expected_epoch = EpochNumber(expected_epoch.as_u64() + 1);
        scenario
            .wait_epoch(expected_epoch.as_u64(), timeout)
            .await
            .expect("wait for epoch 3");
        scenario
            .wait_for_nodes_epoch(&[0], Some(expected_epoch), timeout)
            .await
            .expect("node store epoch catches self-driven epoch");

        scenario.warp_epoch().expect("warp third epoch");
        expected_epoch = EpochNumber(expected_epoch.as_u64() + 1);
        trace!(
            test = "phase1_single_node_advance",
            phase = "self_driven_advance_2",
            expected_epoch = expected_epoch.as_u64()
        );
        scenario
            .wait_epoch(expected_epoch.as_u64(), timeout)
            .await
            .expect("wait for epoch 4");
        scenario
            .wait_for_nodes_epoch(&[0], Some(expected_epoch), timeout)
            .await
            .expect("node store epoch catches second self-driven epoch");
    }

    harness.stop_all().await.expect("stop runtimes");
}
