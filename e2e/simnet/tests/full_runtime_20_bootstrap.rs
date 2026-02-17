use std::time::{Duration, Instant};

use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{ChainFixture, NodeRuntimeMode, SimnetBuilder};
use tape_store::ops::{CommitteeOps, MetaOps};
use tape_store::types::NodeStatus;

#[tokio::test]
async fn full_runtime_20_nodes_bootstrap_and_advance_epoch() {
    let target_epoch;
    let mut harness = SimnetBuilder::new()
        .node_count(20)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(24_000)
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

    harness
        .bootstrap_nodes(0, BasisPoints(100), 1_000, Duration::from_secs(20))
        .await
        .expect("bootstrap nodes");
    assert!(harness.nodes().iter().all(|n| n.is_running()));

    {
        let scenario = harness.scenario();
        let initial_epoch = scenario.read_epoch().await.expect("read initial epoch").id;
        scenario
            .wait_next_quorum(20, Duration::from_secs(20))
            .await
            .expect("committee_next should include all nodes");

        let system = scenario.read_system().await.expect("read system after bootstrap");
        assert_eq!(system.committee.size(), 0);
        assert_eq!(system.committee_next.size(), 20);

        // Force epoch boundary, then advance to move committee_next -> committee.
        scenario.warp_epoch().expect("warp one epoch");
        scenario
            .advance_epoch_any()
            .await
            .expect("advance epoch from any eligible node");
        target_epoch = EpochNumber(initial_epoch.as_u64().saturating_add(1));
        scenario
            .wait_active_epoch(target_epoch, Duration::from_secs(20))
            .await
            .expect("target epoch active");
        scenario
            .wait_for_all_nodes_epoch(Some(target_epoch), Duration::from_secs(20))
            .await
            .expect("all nodes should converge to target epoch");
    }

    // Wait for each node to publish committee data for epoch 1 in MemoryStore.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let ready = harness.nodes().iter().all(|node| {
            let committee = node.context().store.get_committee(target_epoch).ok().flatten();
            matches!(committee, Some(members) if members.len() == 20)
        });
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for target epoch committee in MemoryStore"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // After epoch rotation, nodes should no longer be in recovery-only statuses.
    for node in harness.nodes() {
        let status = node
            .context()
            .store
            .get_node_status()
            .expect("read node status")
            .expect("node status should be set");
        assert!(
            !matches!(
                status,
                NodeStatus::RecoverMetadata
                    | NodeStatus::RecoveryReplay
                    | NodeStatus::RecoveryInProgress { .. }
                    | NodeStatus::PartialReplay { .. }
            ),
            "unexpected recovery status for node {}: {status:?}",
            node.id()
        );
    }

    harness.stop_all().await.expect("stop all runtimes");
}
