use std::time::Duration;

use tape_chain_harness::TEST_EPOCH_DURATION;
use tape_core::types::BasisPoints;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};

#[test]
fn basic_flow() {
    run_simnet_test(basic_flow_inner);
}

async fn basic_flow_inner() {
    let node_count = 20;
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
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let epoch_timeout = Duration::from_secs(TEST_EPOCH_DURATION.0 * 5);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(20))
        .await
        .expect("all nodes active");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("self advance epoch");
    assert_eq!(epoch, 2, "expected epoch 2");

    harness.stop_all().await.expect("stop runtimes");
}
