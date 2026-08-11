//! Verifies that challenge evidence can evict a seated node without a hand-driven proposal.

use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_api::program::tapedrive::track_pda;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;

const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const SILENT_NODE: usize = 0;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;
const STEADY_EPOCH: u64 = 3;

/// Simnet settles two or three rounds per epoch and an epoch takes a minute or
/// two of wall clock, so three consecutive misses accumulate across roughly two
/// epochs, and the proposal and votes need another one or two to land.
const EVICT_TIMEOUT: Duration = Duration::from_secs(600);

/// How often the operator crank re-signs the join and pool advance while the
/// node is dark.
const CRANK_INTERVAL: Duration = Duration::from_secs(2);

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

    // Rewards are weighted by assigned storage and the pool refills each epoch
    // from what reservations paid, so reserve enough capacity for the share
    // arithmetic to clear integer floors. The run's epoch count varies with
    // scheduling, one run reached epoch 15, so the reservation outlives any
    // plausible arc or the dark epoch's pool is empty and its claim is zero.
    let doomed = {
        let scenario = harness.scenario();
        let sdk = scenario.sdk(harness.admin());
        let tape_key = TapeKey::generate();
        sdk.reserve(&tape_key, StorageUnits::mb(1), 32)
            .await
            .expect("reserve a tape for assigned weight");
        sdk.write_raw(&tape_key, &[0x42; 512])
            .await
            .expect("write a raw track");

        let doomed_key = TapeKey::generate();
        sdk.reserve(&doomed_key, StorageUnits::mb(1), 6)
            .await
            .expect("reserve the doomed tape");
        let track = sdk
            .write_raw(&doomed_key, &[0x5A; 512])
            .await
            .expect("write the doomed track");
        (doomed_key, track_pda(track.tape, track.track_number).0)
    };

    // Settle a few epochs in, so rounds have been running against a full group
    // and every node holds a record for every other.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;

    harness
        .stop_nodes(&[SILENT_NODE])
        .await
        .expect("stop the silent node");

    // Delete a track while rounds are settling. The tombstones have to keep
    // every live observer's sample set identical through the deletion, or the
    // group starts refusing each other's honest answers; the clean-record
    // assertions below are what would catch that.
    harness
        .scenario()
        .sdk(harness.admin())
        .delete(&doomed.0, doomed.1)
        .await
        .expect("delete the doomed track mid-round");

    let suspended = wait_eviction_without_proposing(&harness, EVICT_TIMEOUT).await;
    assert_ne!(
        suspended,
        EpochNumber(0),
        "the dark node was never suspended, so nothing originated a proposal"
    );

    // One dark spool must not poison the group. A certificate needs a
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

    // The economics, in the same run. Eviction is forward-looking only: the
    // epoch the node sat dark still pays its member share, the suspension
    // epoch pays nothing, and the seat is joinable again one epoch later.
    let scenario = harness.scenario();
    if scenario.current_epoch_number().await.expect("current epoch") < suspended.0 {
        assert!(
            scenario.join_committee(SILENT_NODE).await.is_err(),
            "a suspended node's join must be refused"
        );
    }

    advance_to_epoch(&harness, suspended, epoch_timeout).await;
    let node = scenario.read_node(SILENT_NODE).await.expect("read the dark node");
    assert!(
        node.latest_advance_epoch < EpochNumber(suspended.0 - 1),
        "the dark epoch was already claimed, the crank raced the suspension"
    );
    let before = node.pool.stake.as_u64();
    scenario
        .advance_pool_ok(SILENT_NODE)
        .await
        .expect("claim the dark epoch");
    let after = scenario
        .read_node(SILENT_NODE)
        .await
        .expect("read the dark node")
        .pool
        .stake
        .as_u64();
    assert!(
        after > before,
        "the dark epoch paid nothing, expected a member share on top of {before}"
    );

    advance_to_epoch(&harness, EpochNumber(suspended.0 + 1), epoch_timeout).await;
    let before = scenario
        .read_node(SILENT_NODE)
        .await
        .expect("read the dark node")
        .pool
        .stake
        .as_u64();
    scenario
        .advance_pool_ok(SILENT_NODE)
        .await
        .expect("cross the suspension epoch");
    let after = scenario
        .read_node(SILENT_NODE)
        .await
        .expect("read the dark node")
        .pool
        .stake
        .as_u64();
    assert_eq!(after, before, "the suspension epoch paid, expected nothing");

    scenario
        .join_committee(SILENT_NODE)
        .await
        .expect("rejoin after the suspension");
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

/// Poll for suspension while keeping the dark node seated without proposing.
async fn wait_eviction_without_proposing(
    harness: &SimnetHarness,
    timeout: Duration,
) -> EpochNumber {
    let start = Instant::now();
    let mut last_crank: Option<Instant> = None;

    loop {
        let suspended = harness
            .scenario()
            .read_node(SILENT_NODE)
            .await
            .expect("read the dark node")
            .suspended_until;
        if suspended != EpochNumber(0) {
            return suspended;
        }

        assert!(
            start.elapsed() < timeout,
            "no eviction landed within {timeout:?} without a hand-driven proposal"
        );

        // Retried across phases: each only has to land once per epoch, whenever
        // that epoch's window is open. Errors are expected outside the windows,
        // and the join failing for good is the suspension the poll then sees.
        if last_crank.is_none_or(|at| at.elapsed() >= CRANK_INTERVAL) {
            let _ = harness.scenario().advance_pool_ok(SILENT_NODE).await;
            let _ = harness.scenario().join_committee(SILENT_NODE).await;
            last_crank = Some(Instant::now());
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
