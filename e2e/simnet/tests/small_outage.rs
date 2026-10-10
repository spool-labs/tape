//! Private clusters on small profiles with one node down.
//! Nodes only fail here and every count is k.

use std::time::{Duration, Instant};

use tape_api::genesis::GenesisConfig;
use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::{ClayParams, ErasureCode, RSParams, RepParams};
use tape_core::thresholds::TrustModel;
use tape_core::track::types::CompressedTrack;
use tape_core::types::BasisPoints;
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

const THREE_OF_FOUR: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
const MIRROR: ErasureCode = ErasureCode::Replication(RepParams::new(2));
const SMALL_CLAY: ErasureCode = ErasureCode::Clay(ClayParams::new(4, 2, 3));

const STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 32;
const EPOCHS: u64 = 3;

/// Epochs a cluster may still advance through after a node stops, before it stalls
const STALL_ATTEMPTS: usize = 4;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const SLICE_TIMEOUT: Duration = Duration::from_secs(120);
const HEAL_TIMEOUT: Duration = Duration::from_secs(180);
const EPOCH_TIMEOUT: Duration = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

// a cluster of exactly n nodes that loses one keeps writing and reading, and
// its epoch stalls until the node's keys return on a new drive
#[test]
fn stalled_epoch() {
    run_simnet_test(stalled_epoch_inner);
}

async fn stalled_epoch_inner() {
    const LOST: usize = 3;

    let node_count = usize::from(THREE_OF_FOUR.n());
    let mut harness = start_cluster(THREE_OF_FOUR, node_count, 1).await;

    let before = payload(256 * 1024);
    let (first, first_track) = write(&harness, &before).await;
    wait_full_group(&harness, first, &first_track, SLICE_TIMEOUT).await;

    harness.stop_nodes(&[LOST]).await.expect("stop a node");

    let after = payload(300 * 1024);
    let (second, second_track) = write(&harness, &after).await;

    wait_stalled_epoch(&harness).await;
    let seated = harness
        .scenario()
        .committee_next_size()
        .await
        .expect("next committee size");
    assert!(seated < node_count, "the next committee is full, so nothing stalls the epoch");
    assert_eq!(read(&harness, first).await, before, "an old track reads on three nodes");
    assert_eq!(read(&harness, second).await, after, "a new track reads on three nodes");

    // The lost node's keys come back on a new drive. Its stake is already
    // active, so it takes its seat and the epoch moves on.
    harness.wipe_nodes(&[LOST]).expect("wipe the node's store");
    harness.start_nodes(&[LOST]).await.expect("restart the node on an empty store");
    harness
        .scenario()
        .self_advance_epoch(EPOCH_TIMEOUT)
        .await
        .expect("the epoch advances with the node back");
    wait_full_group(&harness, first, &first_track, HEAL_TIMEOUT).await;
    wait_full_group(&harness, second, &second_track, HEAL_TIMEOUT).await;
    assert_eq!(read(&harness, first).await, before);
    assert_eq!(read(&harness, second).await, after);

    harness.stop_all().await.expect("stop runtimes");
}

// a 2-copy mirror writes on one node while the other is down, and the other
// returns with a new drive and rebuilds its spool
#[test]
fn mirror_outage() {
    run_simnet_test(|| outage_inner(MIRROR));
}

// the same outage and lost drive on Clay, where a repair reads from d helpers
#[test]
fn clay_outage() {
    run_simnet_test(|| outage_inner(SMALL_CLAY));
}

async fn outage_inner(profile: ErasureCode) {
    // Every node owns a spool in a cluster of exactly n.
    const LOST: usize = 1;

    let node_count = usize::from(profile.n());
    let mut harness = start_cluster(profile, node_count, 1).await;

    let before = payload(256 * 1024);
    let (first, first_track) = write(&harness, &before).await;
    wait_full_group(&harness, first, &first_track, SLICE_TIMEOUT).await;

    harness.stop_nodes(&[LOST]).await.expect("stop a node");

    let after = payload(300 * 1024);
    let (second, second_track) = write(&harness, &after).await;
    assert_eq!(read(&harness, second).await, after, "a track written with a node down reads");

    // The node returns on an empty store. Only the other owners can give it
    // the first track's slice back.
    harness.wipe_nodes(&[LOST]).expect("wipe the node's store");
    harness.start_nodes(&[LOST]).await.expect("restart the wiped node");
    wait_full_group(&harness, first, &first_track, HEAL_TIMEOUT).await;
    wait_full_group(&harness, second, &second_track, HEAL_TIMEOUT).await;

    run_epochs(&harness, &[(first, &before), (second, &after)]).await;

    harness.stop_all().await.expect("stop runtimes");
}

/// A trusted cluster on `profile`, ready to build
fn cluster(profile: ErasureCode, node_count: usize) -> SimnetBuilder {
    SimnetBuilder::new()
        .network(profile, TrustModel::Trusted)
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
}

async fn start_cluster(profile: ErasureCode, node_count: usize, groups: u64) -> SimnetHarness {
    start(cluster(profile, node_count), groups).await
}

/// Build the cluster and start it with every node registered, staked, active
/// and seated in the committee
async fn start(builder: SimnetBuilder, groups: u64) -> SimnetHarness {
    let mut harness = builder.build().expect("build harness");
    let node_count = harness.nodes().len();
    let group_size = usize::from(harness.config().profile.n());
    let all: Vec<usize> = (0..node_count).collect();

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(STAKE).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, groups)
            .await
            .expect("set spool group preferences");
        // Genesis seats one node per spool, and the rest join over the next epochs.
        if node_count > group_size {
            scenario
                .set_committee_size_many(&all, node_count as u64)
                .await
                .expect("set committee size preferences");
        }
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");
    harness
        .scenario()
        .wait_nodes_healthy(HEALTH_TIMEOUT)
        .await
        .expect("nodes healthy");
    harness
        .scenario()
        .wait_committee_size(node_count, EPOCH_TIMEOUT * 2)
        .await
        .expect("every node seated");
    harness
        .scenario()
        .wait_nodes_active(&all, ACTIVE_TIMEOUT)
        .await
        .expect("all nodes active");

    harness
}

/// Write a coded track and check it certified
async fn write(harness: &SimnetHarness, payload: &[u8]) -> (Address, CompressedTrack) {
    let (_, track, written) = harness
        .scenario()
        .upload(harness.admin(), payload, RESERVE_EPOCHS)
        .await
        .expect("write a coded track");
    assert!(written.is_coded(), "the payload must take the coded path");
    assert!(written.is_certified(), "the write should certify");
    assert_eq!(track, written.address());

    (track, written)
}

async fn read(harness: &SimnetHarness, track: Address) -> Vec<u8> {
    harness
        .scenario()
        .download(harness.admin(), &track)
        .await
        .expect("read the track back")
}

/// Advance through the test's epochs, checking every node stays active and
/// every track still reads
async fn run_epochs(harness: &SimnetHarness, tracks: &[(Address, &[u8])]) {
    let all: Vec<usize> = (0..harness.nodes().len()).collect();

    for _ in 0..EPOCHS {
        harness
            .scenario()
            .self_advance_epoch(EPOCH_TIMEOUT)
            .await
            .expect("advance epoch");
        harness
            .scenario()
            .wait_nodes_active(&all, ACTIVE_TIMEOUT)
            .await
            .expect("all nodes active after the epoch");
        for (track, payload) in tracks {
            assert_eq!(read(harness, *track).await, *payload, "the track reads back unchanged");
        }
    }
}

/// Wait until every current owner of the track's group holds its slice
async fn wait_full_group(
    harness: &SimnetHarness,
    track: Address,
    written: &CompressedTrack,
    timeout: Duration,
) {
    let group_size = usize::from(harness.config().profile.n());
    let deadline = Instant::now() + timeout;
    loop {
        let held = harness
            .scenario()
            .count_current_owner_slices(&track, written.group)
            .await
            .expect("count slices");
        if held == group_size {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{held} of {group_size} owners hold their slice"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait for an epoch that stops advancing, and return its number
async fn wait_stalled_epoch(harness: &SimnetHarness) -> u64 {
    // Longer than a healthy epoch takes here, so a slow epoch is never taken for a stalled one.
    let timeout = Duration::from_secs(GenesisConfig::simnet().epoch_duration.0 * 5);

    for _ in 0..STALL_ATTEMPTS {
        let epoch = harness
            .scenario()
            .current_epoch_number()
            .await
            .expect("current epoch");
        let changed = harness.scenario().wait_epoch_change(epoch, timeout).await;
        if changed.is_err() {
            return epoch;
        }
    }
    panic!("the epoch kept advancing without a full committee");
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}
