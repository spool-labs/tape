//! An eviction that no test hand-drives: the challenge mechanism originates it.
//!
//! Every other eviction test opens the vote by calling `propose_eviction` from
//! the scenario, because until the challenge ran there was nothing on a node that
//! could make the first proposal. Here nobody calls it. A node's runtime is
//! stopped while the scenario keeps signing its per-epoch committee join and
//! pool advance, exactly what a seat-keeping operator whose storage went dark
//! would run. It keeps its seat and its spool, its group stops seeing
//! certificates for that spool, and the local rule fires on its own.
//!
//! The crank is the point, not a contrivance. A fully dead node already loses
//! its seat at the next boundary because joining is per-epoch and
//! authority-signed. The seated-but-dark node is the failure only the challenge
//! can catch, and the eviction's teeth are that a suspended node's join is
//! refused on chain.
//!
//! This is also the only place the challenge transport runs for real: answers and
//! attestations over HTTP between live nodes, relayed through the group, with
//! certificates aggregated from live BLS keys.
//!
//! The run ends on the economics: eviction is forward-looking only, so the epoch
//! the node sat dark still pays its member share, the suspension epoch pays
//! nothing, and the seat is joinable again one epoch later.

use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_api::program::tapedrive::track_pda;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;

// The committee holds exactly the group floor, plus a spare to backfill the seat
// the eviction frees.
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
    // The second, throwaway track exists to be deleted mid-round later.
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

    // Go dark. Nothing else is done to this node and nothing proposes on its
    // behalf; from here the mechanism is on its own.
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

    advance_with_spare(&harness, suspended, epoch_timeout).await;
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

    advance_with_spare(&harness, EpochNumber(suspended.0 + 1), epoch_timeout).await;
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

/// Advance to the target epoch with the spare's join cranked.
///
/// The eviction frees a seat at the committee floor and the commit cannot
/// pass while the next committee is short, but the spare's lifecycle only
/// retries its join on phase transitions the stuck commit never produces.
/// The crank is the operator's side of that bargain, until the node retries
/// joining on its own.
async fn advance_with_spare(harness: &SimnetHarness, target: EpochNumber, epoch_timeout: Duration) {
    let scenario = harness.scenario();
    let start = Instant::now();
    loop {
        if scenario.current_epoch_number().await.expect("current epoch") >= target.0 {
            return;
        }
        assert!(
            start.elapsed() < epoch_timeout,
            "the epoch never reached {} with the spare cranked",
            target.0
        );
        let _ = scenario.join_committee(SPARE_NODE).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Poll for the suspension, keeping the dark node seated but never proposing.
///
/// The epochs keep turning on their own, which is what opens the voting windows.
/// The crank is what stops the boundary from solving the problem for us: a node
/// that neither joins nor advances falls out of the next committee and stops
/// being challenged with its record frozen, which is the failure of the earlier
/// version of this test.
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
