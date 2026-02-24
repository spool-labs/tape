use std::time::Duration;

use tape_core::types::BasisPoints;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

#[tokio::test]
async fn node_churn() {
    let mut harness = SimnetBuilder::new()
        .node_count(30)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    harness
        .bootstrap_nodes(0, BasisPoints(100), 1_000, health_timeout)
        .await
        .expect("bootstrap nodes");

    let all: Vec<usize> = (0..30).collect();
    let active_timeout = Duration::from_secs(20);
    let advance_timeout = Duration::from_secs(30);

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active");
    }

    {
        let scenario = harness.scenario();
        scenario
            .self_advance_epoch(advance_timeout)
            .await
            .expect("self advance epoch");
    }

    harness.stop_all().await.expect("stop runtimes");
}
