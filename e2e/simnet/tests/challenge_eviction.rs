//! An eviction that no test hand-drives: the challenge mechanism originates it.
//!
//! Every other eviction test opens the vote by calling `propose_eviction` from
//! the scenario, because until the challenge ran there was nothing on a node that
//! could make the first proposal. Here nobody calls it. A node is stopped, its
//! group stops seeing certificates for the spool it holds, and the local rule
//! fires on its own.
//!
//! This is also the only place the challenge transport runs for real: answers and
//! attestations over HTTP between live nodes, relayed through the group, with
//! certificates aggregated from live BLS keys.

use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

// The committee holds exactly the group floor, plus a spare to backfill the seat
// the eviction frees.
const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const TARGET_GROUPS: u64 = 1;
const SILENT_NODE: usize = 0;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;
const STEADY_EPOCH: u64 = 3;

/// A round fires every 12 slots on a 20-second epoch, and the local rule needs
/// three consecutive misses, so the earliest a proposal can appear is about
/// fifteen seconds after the node goes quiet. The rest is the voting window.
const EVICT_TIMEOUT: Duration = Duration::from_secs(180);

#[test]
fn challenge_eviction() {
    run_simnet_test(challenge_eviction_inner);
}

async fn challenge_eviction_inner() {
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

    // Settle a few epochs in, so rounds have been running against a full group
    // and every node holds a record for every other.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;

    // Go quiet. Nothing else is done to this node and nothing proposes on its
    // behalf; from here the mechanism is on its own.
    harness
        .stop_nodes(&[SILENT_NODE])
        .await
        .expect("stop the silent node");

    let suspended = wait_eviction_without_proposing(&harness, EVICT_TIMEOUT).await;
    assert_ne!(
        suspended,
        EpochNumber(0),
        "the silent node was never suspended, so nothing originated a proposal"
    );

    // One silent spool must not poison the group. A certificate needs a
    // supermajority, not unanimity, so the nodes that stayed up keep certifying
    // for each other and none of them collects an eviction.
    for node in 1..COMMITTEE_NODES {
        let other = harness
            .scenario()
            .read_node(node)
            .await
            .expect("read node")
            .suspended_until;
        assert_eq!(
            other,
            EpochNumber(0),
            "node {node} was suspended although it answered every round"
        );
    }
}

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

/// Poll for the suspension, advancing epochs but never proposing.
///
/// The absence of a `propose_eviction` call here is the whole point of the test.
async fn wait_eviction_without_proposing(
    harness: &SimnetHarness,
    timeout: Duration,
) -> EpochNumber {
    let start = Instant::now();
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    loop {
        let suspended = harness
            .scenario()
            .read_node(SILENT_NODE)
            .await
            .expect("read the silent node")
            .suspended_until;
        if suspended != EpochNumber(0) {
            return suspended;
        }

        assert!(
            start.elapsed() < timeout,
            "no eviction landed within {timeout:?} without a hand-driven proposal"
        );

        // A vote only lands inside its window, so keep the epochs turning.
        let _ = harness.scenario().self_advance_epoch(epoch_timeout).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
