use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder};

#[test]
fn builder_creates_requested_number_of_nodes() {
    let harness = SimnetBuilder::new()
        .node_count(3)
        .runtime_mode(NodeRuntimeMode::Disabled)
        .build()
        .expect("build harness");

    assert_eq!(harness.nodes().len(), 3);
}

#[tokio::test]
async fn start_stop_noop_when_runtime_disabled() {
    let mut harness = SimnetBuilder::new()
        .node_count(2)
        .runtime_mode(NodeRuntimeMode::Disabled)
        .build()
        .expect("build harness");

    harness.start_all().await.expect("start_all");
    assert!(harness.nodes().iter().all(|n| !n.is_running()));

    harness.stop_all().await.expect("stop_all");
}
