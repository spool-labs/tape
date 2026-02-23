use std::time::Duration;

use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{ChainFixture, NodeRuntimeMode, SimnetBuilder};
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

fn env_timeout_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::test]
async fn full_runtime_bft_epoch3() {
    let mut harness = SimnetBuilder::new()
        .node_count(31)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        //.slot_advance_per_tx(1)
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

    let honest;
    let targets;
    {
        let scenario = harness.scenario();
        targets = scenario.bft_targets();
        assert_eq!(targets.total_nodes, 31);
        assert_eq!(targets.max_faulty, 10);
        assert_eq!(targets.min_correct, 21);
        assert_eq!(targets.min_for_advance, 21);

        honest = scenario.honest_nodes();
        assert_eq!(honest.len(), targets.min_for_advance);

        scenario.init_system(0).await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario
            .stake_many(0, &honest, 1_000)
            .await
            .expect("stake honest nodes");
        scenario
            .pool_many(0, &honest)
            .await
            .expect("pool honest nodes");
        scenario
            .join_many(0, &honest)
            .await
            .expect("join honest nodes");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let final_epoch;
    {
        let scenario = harness.scenario();
        let advance_timeout =
            Duration::from_secs(env_timeout_secs("SIMNET_ADVANCE_TIMEOUT_SECS", 360));
        let active_timeout =
            Duration::from_secs(env_timeout_secs("SIMNET_ACTIVE_TIMEOUT_SECS", 30));
        scenario
            .wait_nodes_healthy(Duration::from_secs(30))
            .await
            .expect("nodes healthy");
        scenario
            .wait_next_bft(Duration::from_secs(30))
            .await
            .expect("bft next quorum");
        scenario
            .advance_to_epoch(3, 0, &honest, advance_timeout)
            .await
            .expect("advance to epoch 3");
        scenario
            .wait_active_epoch(EpochNumber(3), active_timeout)
            .await
            .expect("epoch 3 active");

        let epoch = scenario.read_epoch().await.expect("read final epoch");
        final_epoch = epoch.id;

        let system = scenario.read_system().await.expect("read system");
        assert!(
            system.committee.size() >= targets.min_for_advance,
            "committee too small: {} < {}",
            system.committee.size(),
            targets.min_for_advance
        );
    }

    for &index in &honest {
        let node = harness.node(index).expect("honest node exists");
        let current = Some(node.context().chain_state.load().epoch);
        assert_eq!(current, Some(final_epoch), "honest node {index} epoch mismatch");

        let status = node
            .context()
            .store
            .get_node_status()
            .expect("read node status")
            .unwrap_or(NodeStatus::Standby);
        assert!(
            !matches!(
                status,
                NodeStatus::RecoverMetadata
                    | NodeStatus::RecoveryReplay
                    | NodeStatus::RecoveryInProgress { .. }
                    | NodeStatus::PartialReplay { .. }
            ),
            "unexpected recovery status for node {index}: {status:?}"
        );
    }

    harness.stop_all().await.expect("stop runtimes");
}
