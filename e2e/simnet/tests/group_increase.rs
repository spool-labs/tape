//! The spool group count rising from one to TARGET_GROUPS with a blob in place.
//!
//! A track keeps the virtual group it was stamped with. At the higher count
//! that virtual group belongs to one of the new groups, and a new group with
//! data starts on the nodes that already hold that data, position by
//! position. So the blob's holders are the same before and after, each has
//! its slice the moment the count comes live, and nothing is fetched.
//!
//! The blob has to land in the first epoch. A group's size takes the tracks
//! certified before the epoch that votes it, and a new group that counts as
//! empty gets a fresh fill, whose nodes would then pull the blob from the old
//! holders.

use std::time::Duration;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::spooler::VirtualGroup;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetScenario, TAPENET_GROUP_SIZE, run_simnet_test,
};

const TARGET_GROUPS: u64 = 20;

/// Blob lifetime, in epochs. The test runs five.
const BLOB_EPOCHS: u64 = 16;

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
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
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

    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (_tape_key, track_address, track) = scenario
        .upload(harness.admin(), &data, BLOB_EPOCHS)
        .await
        .expect("upload blob");
    assert!(track.is_certified(), "uploaded blob track should be certified");
    assert_eq!(
        scenario.read_system().await.expect("read system").current_epoch.0,
        1,
        "the blob has to land in the first epoch to count before the grown layout is voted"
    );
    let holders_before = group_holders(&scenario, track.group).await;

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

        // The blob's holders stayed put through the growth and hold their
        // slices with nothing to fetch.
        let held = scenario
            .count_current_owner_slices(&track_address, track.group)
            .await
            .expect("count current owner slices");
        assert_eq!(held, TAPENET_GROUP_SIZE, "a holder is missing its slice at epoch {epoch}");
        assert_eq!(
            group_holders(&scenario, track.group).await,
            holders_before,
            "the blob's group sits on the nodes that held the blob before, position by position"
        );

        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active after epoch advance");
        assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
    }

    let reread = scenario
        .download(harness.admin(), &track_address)
        .await
        .expect("download blob after growth");
    assert_eq!(reread, data, "download after growth should match upload");

    harness.stop_all().await.expect("stop runtimes");
}

/// The nodes of the live group that carries a virtual group, by position.
async fn group_holders(scenario: &SimnetScenario<'_>, group: VirtualGroup) -> Vec<Address> {
    let system = scenario.read_system().await.expect("read system");
    let live_group = system.live_group(group).expect("live group");
    let account = scenario
        .read_group(system.current_epoch, live_group)
        .await
        .expect("read group");

    account.spools.iter().map(|spool| spool.node).collect()
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
            TAPENET_GROUP_SIZE,
            "group {index} is not fully assigned"
        );
    }
}
