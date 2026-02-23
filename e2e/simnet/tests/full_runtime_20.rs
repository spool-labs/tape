use std::time::Duration;

use tape_core::types::BasisPoints;
use tape_e2e_simnet::{ChainFixture, NodeRuntimeMode, SimnetBuilder};
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

#[tokio::test]
async fn full_runtime_20_nodes_register_and_refresh_state() {
    let expected_epoch;
    let mut harness = SimnetBuilder::new()
        .node_count(20)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(23_000)
        .slot_advance_per_tx(1)
        .build()
        .expect("build harness");

    {
        let scenario = harness.scenario();
        let workspace = scenario.workspace_root().expect("workspace root");
        let required = [
            ChainFixture::deploy_path(&workspace, "tapedrive"),
            ChainFixture::deploy_path(&workspace, "token"),
            ChainFixture::deploy_path(&workspace, "exchange"),
            ChainFixture::deploy_path(&workspace, "staking"),
            ChainFixture::external_program_path(&workspace, "mpl_token_metadata"),
        ];
        let missing: Vec<_> = required.iter().filter(|p| !p.exists()).collect();
        assert!(
            missing.is_empty(),
            "missing required simnet program artifacts: {:?}",
            missing
        );
    }

    {
        let scenario = harness.scenario();
        scenario.init_system(0).await.expect("init system");
        let chain_epoch = scenario.read_epoch().await.expect("read epoch").id;
        expected_epoch = chain_epoch;

        let signatures = scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        assert_eq!(signatures.len(), 20);

        // Join is expected to fail in this baseline fixture because no node has active stake yet.
        let join_results = scenario.join_network().await;
        assert_eq!(join_results.len(), 20);
        assert!(join_results.iter().all(|r| r.result.is_err()));
    }

    harness.start_all().await.expect("start all runtimes");
    assert!(harness.nodes().iter().all(|n| n.is_running()));

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(Duration::from_secs(20))
            .await
            .expect("nodes should expose healthy http endpoints");

        let system = scenario.read_system().await.expect("read system");
        assert_eq!(system.committee.size(), 0);
        assert_eq!(system.committee_next.size(), 0);

        scenario
            .wait_for_all_nodes_epoch(Some(expected_epoch), Duration::from_secs(20))
            .await
            .expect("nodes should refresh on-chain epoch into memory store");
    }

    for node in harness.nodes() {
        let epoch = Some(node.context().chain_state.load().epoch);
        assert_eq!(epoch, Some(expected_epoch));

        let status = node
            .context()
            .store
            .get_node_status()
            .expect("read node status");
        assert_eq!(status, Some(NodeStatus::Standby));
    }

    harness.stop_all().await.expect("stop all runtimes");
}
