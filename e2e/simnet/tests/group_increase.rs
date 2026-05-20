use std::time::Duration;

use tape_api::program::EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};

const TARGET_GROUPS: u64 = 20;

#[test]
fn group_increase() {
    run_simnet_test(group_increase_inner);
}

async fn group_increase_inner() {
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
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(EPOCH_DURATION as u64 * 5);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");
    assert_group_counts(&scenario, 1, 1).await;

    let epoch2 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch2, 2, "expected epoch 2");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at epoch 2");
    assert_group_counts(&scenario, TARGET_GROUPS, 1).await;

    for expected_epoch in 3..=5 {
        let epoch = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("self advance epoch");

        assert_eq!(epoch, expected_epoch, "unexpected epoch after advance");

        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active after epoch advance");
        assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
    }

    harness.stop_all().await.expect("stop runtimes");
}

async fn assert_group_counts(
    scenario: &SimnetScenario<'_>,
    expected_target: u64,
    expected_live: u64,
) {
    let system = scenario.read_system().await.expect("read system");

    assert_eq!(
        system.target_group_count, expected_target,
        "unexpected target group count"
    );
    assert_eq!(
        system.live_group_count, expected_live,
        "unexpected live group count"
    );

    let groups = scenario
        .read_groups(system.current_epoch, expected_live)
        .await
        .expect("read current groups");

    assert_eq!(groups.len(), expected_live as usize, "unexpected group count");

    for (index, group) in groups.iter().enumerate() {
        assert_eq!(
            group.epoch, system.current_epoch,
            "unexpected group epoch for group {index}"
        );
        assert_eq!(
            group.id.0, index as u64,
            "unexpected group id for group {index}"
        );
        assert_eq!(
            group
                .spools
                .iter()
                .filter(|spool| spool.node != Address::default())
                .count(),
            GROUP_SIZE,
            "group {index} is not fully assigned"
        );
    }
}
