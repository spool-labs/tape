use std::collections::HashSet;
use std::time::Duration;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::NodeStatus;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};

const TARGET_GROUPS: u64 = 10;

#[test]
fn committee_increase() {
    run_simnet_test(committee_increase_inner);
}

async fn committee_increase_inner() {
    let node_count = 25;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    let genesis_committee: Vec<usize> = (0..20).collect();
    let late_nodes: Vec<usize> = (20..25).collect();
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
        scenario
            .set_committee_size_many(&all, node_count as u64)
            .await
            .expect("set committee size preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&genesis_committee, active_timeout)
        .await
        .expect("genesis committee active");
    assert_eq!(
        scenario.committee_size().await.expect("committee size"),
        genesis_committee.len(),
        "unexpected genesis committee size"
    );

    let late_node_addresses = late_nodes
        .iter()
        .map(|&i| Address::from(scenario.node_address(i)))
        .collect::<HashSet<_>>();

    let genesis_owners = assert_group_owners(&scenario, 1, 1).await;
    assert!(
        late_node_addresses
            .iter()
            .all(|node| !genesis_owners.contains(node)),
        "late nodes should not own genesis spools"
    );

    let epoch2 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch2, 2, "expected epoch 2");
    scenario
        .wait_nodes_active(&genesis_committee, active_timeout)
        .await
        .expect("genesis committee active at epoch 2");
    for &node in &late_nodes {
        assert_eq!(
            scenario.node_status(node),
            Some(NodeStatus::Standby),
            "late node {node} should not be active until epoch 3"
        );
    }
    let system = scenario.read_system().await.expect("read system");
    assert_eq!(
        system.committee_size, node_count as u64,
        "epoch 2 should carry the new committee-size preference"
    );
    assert_eq!(
        scenario.committee_size().await.expect("committee size"),
        genesis_committee.len(),
        "unexpected epoch 2 committee size"
    );
    scenario
        .wait_next_quorum(node_count, active_timeout)
        .await
        .expect("epoch 3 candidate committee reached expanded size");
    assert_eq!(
        scenario
            .committee_next_size()
            .await
            .expect("next committee size"),
        node_count,
        "unexpected epoch 3 candidate committee size"
    );
    assert_group_owners(&scenario, TARGET_GROUPS, 1).await;

    let epoch3 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 3");
    assert_eq!(epoch3, 3, "expected epoch 3");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at epoch 3");
    assert_eq!(
        scenario.committee_size().await.expect("committee size"),
        node_count,
        "unexpected epoch 3 committee size"
    );

    let expanded_owners = assert_group_owners(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
    assert_ne!(
        genesis_owners, expanded_owners,
        "expected spool ownership to change after committee increase"
    );
    for node in &late_node_addresses {
        assert!(
            expanded_owners.contains(node),
            "late node {node} did not receive any spool ownership"
        );
    }

    harness.stop_all().await.expect("stop runtimes");
}

async fn assert_group_owners(
    scenario: &SimnetScenario<'_>,
    expected_target: u64,
    expected_live: u64,
) -> Vec<Address> {
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

    let mut owners = Vec::with_capacity(expected_live as usize * GROUP_SIZE);
    for (index, group) in groups.iter().enumerate() {
        assert_eq!(
            group.epoch, system.current_epoch,
            "unexpected group epoch for group {index}"
        );
        assert_eq!(
            group.id.as_u64(), index as u64,
            "unexpected group id for group {index}"
        );

        for spool in &group.spools {
            assert_ne!(
                spool.node,
                Address::default(),
                "group {index} has empty spool owner"
            );
            owners.push(spool.node);
        }
    }

    owners
}
