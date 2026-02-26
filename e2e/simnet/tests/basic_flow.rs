use std::time::Duration;

use tape_core::types::BasisPoints;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

#[tokio::test]
async fn basic_flow() {
    let mut harness = SimnetBuilder::new()
        .node_count(30)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    harness
        .bootstrap_nodes(BasisPoints(100), 1_000, health_timeout)
        .await
        .expect("bootstrap nodes");

    let all: Vec<usize> = (0..30).collect();
    let scenario = harness.scenario();
    scenario
        .wait_nodes_active(&all, Duration::from_secs(20))
        .await
        .expect("all nodes active");
    scenario
        .self_advance_epoch(Duration::from_secs(30))
        .await
        .expect("self advance epoch");

    harness.stop_all().await.expect("stop runtimes");
}
