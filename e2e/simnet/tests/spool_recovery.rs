use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};

const TARGET_GROUPS: u64 = 5;
const NODE_COUNT: usize = 25;

#[test]
fn spool_recovery() {
    run_simnet_test(spool_recovery_inner);
}

async fn spool_recovery_inner() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let genesis_committee: Vec<usize> = (0..GROUP_SIZE).collect();
    let late_nodes: Vec<usize> = (GROUP_SIZE..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario
            .stake_many(&genesis_committee, 1_000)
            .await
            .expect("stake genesis nodes");
        scenario
            .stake_many(&late_nodes, 3_000)
            .await
            .expect("stake late nodes");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario
            .set_committee_size_many(&all, NODE_COUNT as u64)
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
    let recovery_timeout = Duration::from_secs(120);
    let expansion_timeout = epoch_timeout * 2;
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&genesis_committee, active_timeout)
        .await
        .expect("genesis committee active");
    assert_group_counts(&scenario, 1, 1).await;

    // Expand first. Seating is redrawn every epoch and is not promised in any
    // one of them, so this waits for the size rather than naming an epoch. The
    // recovery checks below run after the upload so they cannot drift away from
    // it while this waits.
    scenario
        .wait_committee_size(NODE_COUNT, expansion_timeout)
        .await
        .expect("committee reached the expanded size");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active once the committee expanded");
    assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;

    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (_tape_key, track_address, track) = scenario
        .upload(harness.admin(), &data, 6)
        .await
        .expect("upload blob");

    assert!(track.is_coded(), "uploaded track should be a blob track");
    assert!(
        track.is_certified(),
        "uploaded blob track should be certified"
    );

    let upload_owners = group_owners(&scenario, track.group).await;
    assert!(
        includes_late_owner(&scenario, &late_nodes, &upload_owners),
        "expected at least one late node to own the track group after expansion"
    );
    wait_current_owner_slices(
        &scenario,
        &track_address,
        track.group,
        GROUP_SIZE,
        recovery_timeout,
    )
    .await
    .expect("owners store all blob slices");

    // One turnover is all the recovery path needs to prove: whoever owns the
    // group next must fetch what it does not already hold.
    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance one epoch");

    wait_current_owner_slices(
        &scenario,
        &track_address,
        track.group,
        GROUP_SIZE,
        recovery_timeout,
    )
    .await
    .expect("owners after the turnover keep all blob slices available");

    let reread = scenario
        .download(harness.admin(), &track_address)
        .await
        .expect("download blob after the turnover");
    assert_eq!(reread, data, "post-turnover download should match upload");

    harness.stop_all().await.expect("stop runtimes");
}

async fn wait_current_owner_slices(
    scenario: &SimnetScenario<'_>,
    track: &Address,
    group: GroupIndex,
    expected: usize,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let observed = scenario
            .count_current_owner_slices(track, group)
            .await
            .expect("count current owner slices");
        if observed == expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "timed out waiting for current group owners to hold {expected} slices, observed {observed}"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn group_owners(scenario: &SimnetScenario<'_>, group: GroupIndex) -> Vec<Address> {
    let system = scenario.read_system().await.expect("read system");
    let group = scenario
        .read_group(system.current_epoch, group)
        .await
        .expect("read group");

    group.spools.iter().map(|spool| spool.node).collect()
}

fn includes_late_owner(
    scenario: &SimnetScenario<'_>,
    late_nodes: &[usize],
    owners: &[Address],
) -> bool {
    let late_node_addresses = late_nodes
        .iter()
        .map(|&i| Address::from(scenario.node_address(i)))
        .collect::<HashSet<_>>();

    owners.iter().any(|owner| late_node_addresses.contains(owner))
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
}
