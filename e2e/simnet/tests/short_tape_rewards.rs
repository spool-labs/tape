//! e2e regression for the short-lived-tape assignment-weight fix.
use std::time::{Duration, Instant};

use rpc_client::RpcClient;
use tape_api::program::tapedrive::track_pda;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_crypto::{Address, hash};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::{ObjectInfoOps, TrackDataOps, TrackOps};

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const POOL: usize = 0;
const COMMISSION: BasisPoints = BasisPoints(1_000);

const DATA_EPOCH: EpochNumber = EpochNumber(2);
const CUTOFF_EPOCH: EpochNumber = EpochNumber(3);
const ASSIGNED_EPOCH: EpochNumber = EpochNumber(4);
const FIRST_CLAIM_EPOCH: EpochNumber = EpochNumber(5);

const SHORT_LIFETIME: u64 = 2;

#[test]
fn short_lived_tape_earns_rewards() {
    run_simnet_test(run);
}

async fn run() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(0)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(COMMISSION)
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

    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    // Advance to the data epoch and close the activation rate span.
    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to data epoch");
    assert_eq!(epoch, DATA_EPOCH.0, "unexpected data epoch");
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("close activation rate span");

    // Write a small track into a deliberately SHORT-lived tape (expiry = 4).
    let track = write_short_tape_data(&harness).await;
    wait_track(&harness, track, active_timeout).await;

    // Advance through the cutoff epoch into the assigned epoch.
    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to cutoff epoch");
    assert_eq!(epoch, CUTOFF_EPOCH.0, "unexpected cutoff epoch");
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("advance target pool at cutoff");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to assigned epoch");
    assert_eq!(epoch, ASSIGNED_EPOCH.0, "unexpected assigned epoch");

    // the short-lived track produced nonzero assignment weight. This is
    // the value that was 0 for the whole life of any <= 2-epoch tape pre-fix.
    let pool_node = Address::from(scenario.node_address(POOL));
    let assigned =
        wait_member_assigned(&scenario, ASSIGNED_EPOCH, pool_node, active_timeout).await;
    assert!(
        assigned > StorageUnits::zero(),
        "short-lived tape must yield nonzero assignment weight (member.assigned was 0 pre-fix)"
    );

    // that weight turns into real storage rewards once the pool advances
    // past the assigned epoch.
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("advance target pool at assigned epoch");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to first claim epoch");
    assert_eq!(epoch, FIRST_CLAIM_EPOCH.0, "unexpected claim epoch");

    let node = wait_pool_advanced(&harness, POOL, FIRST_CLAIM_EPOCH, active_timeout).await;
    let earned = node.pool.commission.saturating_add(node.pool.rewards);
    assert!(
        earned > TAPE::zero(),
        "short-lived tape storage must earn rewards (commission {} + rewards {}); both were 0 pre-fix",
        node.pool.commission,
        node.pool.rewards,
    );

    drop(scenario);
    harness.stop_all().await.expect("stop harness");
}

async fn write_short_tape_data(harness: &SimnetHarness) -> Address {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let data = vec![0x42; 512];

    sdk.reserve(&tape_key, StorageUnits::mb(1), SHORT_LIFETIME)
        .await
        .expect("reserve short-lived tape");

    let track = sdk
        .write_raw(&tape_key, hash::hash(b"short-tape-rewards"), &data)
        .await
        .expect("write raw track");
    assert_eq!(track.group, GroupIndex(0), "unexpected raw track group");

    track_pda(track.tape, track.track_number).0
}

/// Poll the target committee until the pool node has nonzero assigned weight, or
/// the timeout elapses (in which case the last-read value 0 is returned so
/// the caller's assertion produces a clear message instead of a hang).
async fn wait_member_assigned(
    scenario: &SimnetScenario<'_>,
    epoch: EpochNumber,
    node: Address,
    timeout: Duration,
) -> StorageUnits {
    let start = Instant::now();
    let mut last = StorageUnits::zero();
    loop {
        if let Ok(members) = scenario.read_committee(epoch).await {
            if let Some(member) = members.iter().find(|member| member.node == node) {
                if member.assigned > StorageUnits::zero() {
                    return member.assigned;
                }
                last = member.assigned;
            }
        }
        if start.elapsed() >= timeout {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_pool_advanced(
    harness: &SimnetHarness,
    node_index: usize,
    current_epoch: EpochNumber,
    timeout: Duration,
) -> tape_api::state::Node {
    let expected_advance = current_epoch.prev();
    let start = Instant::now();

    loop {
        let node = read_node(harness, node_index).await;
        if node.latest_advance_epoch >= expected_advance {
            return node;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for pool {node_index} to advance at epoch {}, latest {}",
            current_epoch.0,
            node.latest_advance_epoch.0,
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn read_node(harness: &SimnetHarness, node_index: usize) -> tape_api::state::Node {
    let client = RpcClient::from_rpc(harness.chain().rpc().clone());
    let node = Address::from(harness.scenario().node_address(node_index));
    client
        .get_node_by_address(&node)
        .await
        .expect("read node")
}

async fn wait_track(harness: &SimnetHarness, track: Address, timeout: Duration) {
    let start = Instant::now();
    loop {
        let running = harness
            .nodes()
            .iter()
            .filter(|node| node.is_running())
            .count();
        let seen = harness
            .nodes()
            .iter()
            .filter(|node| node.is_running())
            .filter(|node| {
                let store = &node.context().store;
                let has_track = store.has_track(track).expect("read track");
                let has_data = store.has_track_data(track).expect("read track data");
                let has_object_info =
                    store.has_object_info(track).expect("read object info");
                has_track && has_data && has_object_info
            })
            .count();

        if seen == running {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for track {track} on running nodes, seen {seen}/{running}"
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
