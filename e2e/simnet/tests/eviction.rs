use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::NodeStatus;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

// The committee holds exactly the group floor. A lower stake spare stands by so
// the committee can refill after an eviction, and so the evicted node can later
// reclaim its seat by out-staking the spare.
const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const TARGET_GROUPS: u64 = 1;
const EVICT_NODE: usize = 0;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;
const STEADY_EPOCH: u64 = 3;

#[test]
fn eviction() {
    run_simnet_test(eviction_inner);
}

async fn eviction_inner() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let committee: Vec<usize> = (0..COMMITTEE_NODES).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let evict_timeout = Duration::from_secs(90);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario
            .stake_many(&committee, SEATED_STAKE)
            .await
            .expect("stake committee nodes");
        scenario
            .stake_node(SPARE_NODE, SPARE_STAKE)
            .await
            .expect("stake spare node");
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

    harness
        .scenario()
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    harness
        .scenario()
        .wait_nodes_active(&committee, active_timeout)
        .await
        .expect("committee active");

    // Settle into a steady state a few epochs in.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;

    let target = Address::from(harness.scenario().node_address(EVICT_NODE));

    // Stop the target so every voter's own probe fails, then open the vote
    // with a single permissionless proposal. Committee nodes observe the
    // proposal on-chain, probe the target themselves, and sign only because
    // the probe fails. Landing takes a supermajority of groups and sets
    // suspended_until while removing the target from the next committee.
    harness
        .stop_nodes(&[EVICT_NODE])
        .await
        .expect("stop target node");
    let target_epoch = wait_eviction_landed(&harness, evict_timeout).await;

    // The target is absent from the committee the vote targeted.
    let next_members = harness
        .scenario()
        .read_committee(target_epoch)
        .await
        .expect("read target committee");
    assert!(
        next_members.iter().all(|member| member.node != target),
        "evicted node still seated in committee {}",
        target_epoch.0
    );

    // Bring the target back so it can observe its suspension and later rejoin.
    harness
        .start_nodes_with_retry(&[EVICT_NODE], 3, Duration::from_millis(200))
        .await
        .expect("restart evicted node");
    harness
        .scenario()
        .wait_node_healthy(EVICT_NODE, health_timeout)
        .await
        .expect("evicted node healthy after restart");

    // Advance into the target epoch and confirm the node dropped out of the
    // active committee. The spare backfills the freed seat.
    advance_to_epoch(&harness, target_epoch, epoch_timeout).await;
    assert_eq!(
        harness.scenario().node_status(EVICT_NODE),
        Some(NodeStatus::Standby),
        "evicted node should be standby in the target epoch"
    );
    let active_members = harness
        .scenario()
        .read_committee(target_epoch)
        .await
        .expect("read active committee");
    assert!(
        active_members.iter().all(|member| member.node != target),
        "evicted node still seated in the active committee"
    );
    assert_eq!(
        active_members.len(),
        COMMITTEE_NODES,
        "committee should stay full after the spare backfills"
    );

    // After the one-epoch cooldown suspended_until passes and the node reclaims
    // a seat by out-staking the spare.
    let rejoin_epoch = target_epoch.next();
    advance_to_epoch(&harness, rejoin_epoch, epoch_timeout).await;
    harness
        .scenario()
        .wait_nodes_active(&[EVICT_NODE], active_timeout)
        .await
        .expect("evicted node rejoins after cooldown");
    let rejoin_members = harness
        .scenario()
        .read_committee(rejoin_epoch)
        .await
        .expect("read rejoin committee");
    assert!(
        rejoin_members.iter().any(|member| member.node == target),
        "node did not rejoin committee {} after cooldown",
        rejoin_epoch.0
    );

    harness.stop_all().await.expect("stop runtimes");
}

// Advance epochs until the current epoch reaches the target.
async fn advance_to_epoch(harness: &SimnetHarness, target: EpochNumber, epoch_timeout: Duration) {
    let scenario = harness.scenario();
    loop {
        if scenario.current_epoch_number().await.expect("current epoch") >= target.0 {
            return;
        }
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance epoch toward target");
    }
}

// Propose the eviction and poll the target node account until its suspension
// lands, returning the epoch it is suspended through. A proposal expires with
// its voting epoch, so the poll re-proposes each round; a duplicate proposal
// is the same PDA and is rejected, which the poll ignores.
async fn wait_eviction_landed(harness: &SimnetHarness, timeout: Duration) -> EpochNumber {
    let start = Instant::now();
    loop {
        let _ = harness.scenario().propose_eviction(EVICT_NODE).await;

        let suspended = harness
            .scenario()
            .read_node(EVICT_NODE)
            .await
            .expect("read target node")
            .suspended_until;
        if suspended != EpochNumber(0) {
            return suspended;
        }
        assert!(
            start.elapsed() < timeout,
            "eviction did not land within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
