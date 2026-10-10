//! Healthy private clusters on small profiles, where nodes only fail and every count is k.

use std::time::{Duration, Instant};

use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::{ClayParams, ErasureCode, RSParams};
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::thresholds::TrustModel;
use tape_core::track::types::CompressedTrack;
use tape_core::types::BasisPoints;
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

const THREE_OF_FOUR: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
const WIDE_CLAY: ErasureCode = ErasureCode::Clay(ClayParams::new(10, 7, 9));

const STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 32;
const EPOCHS: u64 = 3;

/// Epochs a new group count may take to go live
const GROWTH_EPOCHS: usize = 4;

/// A write lands in a group picked by hash, so reaching every group takes a few
const WRITE_ATTEMPTS: usize = 24;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const SLICE_TIMEOUT: Duration = Duration::from_secs(120);
const EPOCH_TIMEOUT: Duration = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

// a healthy 3-of-4 cluster writes, certifies, fills every spool and keeps the
// data readable across epochs
#[test]
fn three_of_four() {
    run_simnet_test(three_of_four_inner);
}

async fn three_of_four_inner() {
    let node_count = usize::from(THREE_OF_FOUR.n());
    let mut harness = start_cluster(THREE_OF_FOUR, node_count, 1).await;

    let data = payload(256 * 1024);
    let (track, written) = write(&harness, &data).await;

    // The upload returns at k slices, and the last owner still gets its own.
    wait_full_group(&harness, track, &written, SLICE_TIMEOUT).await;

    run_epochs(&harness, &[(track, &data)]).await;

    harness.stop_all().await.expect("stop runtimes");
}

// a second group goes live on the same four nodes and takes writes
#[test]
fn add_group() {
    run_simnet_test(add_group_inner);
}

async fn add_group_inner() {
    const GROUPS: u64 = 2;

    let node_count = usize::from(THREE_OF_FOUR.n());
    let mut harness = start_cluster(THREE_OF_FOUR, node_count, GROUPS).await;

    wait_groups(&harness, GROUPS).await;

    let data = payload(256 * 1024);
    let tracks = write_each_group(&harness, GROUPS, &data).await;
    for (track, written) in &tracks {
        wait_full_group(&harness, *track, written, SLICE_TIMEOUT).await;
        assert_eq!(read(&harness, *track).await, data);
    }

    harness.stop_all().await.expect("stop runtimes");
}

// twelve nodes on a 10-wide profile: the groups between them use more nodes
// than one group has spools
#[test]
fn wide_committee() {
    run_simnet_test(wide_committee_inner);
}

async fn wide_committee_inner() {
    const GROUPS: u64 = 3;
    const SPARES: usize = 2;

    let group_size = usize::from(WIDE_CLAY.n());
    let mut harness = start_cluster(WIDE_CLAY, group_size + SPARES, GROUPS).await;

    wait_groups(&harness, GROUPS).await;

    let mut used: Vec<usize> = Vec::new();
    for group in 0..GROUPS {
        used.extend(owners(&harness, VirtualGroup(group)).await);
    }
    used.sort_unstable();
    used.dedup();
    assert!(
        used.len() > group_size,
        "{GROUPS} groups sit on {} nodes, no more than one group's {group_size}",
        used.len()
    );

    let data = payload(256 * 1024);
    let tracks = write_each_group(&harness, GROUPS, &data).await;
    for (track, written) in &tracks {
        wait_full_group(&harness, *track, written, SLICE_TIMEOUT).await;
    }

    let written: Vec<(Address, &[u8])> = tracks
        .iter()
        .map(|(track, _)| (*track, data.as_slice()))
        .collect();
    run_epochs(&harness, &written).await;

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

/// Write tracks until every group holds one, and return one track per group
async fn write_each_group(
    harness: &SimnetHarness,
    groups: u64,
    payload: &[u8],
) -> Vec<(Address, CompressedTrack)> {
    let mut tracks: Vec<(Address, CompressedTrack)> = Vec::new();
    let mut reached: Vec<GroupIndex> = Vec::new();

    for _ in 0..WRITE_ATTEMPTS {
        let (track, written) = write(harness, payload).await;

        // A track's leaf carries its virtual group. The live group that holds
        // it is what has to differ from one track to the next.
        let system = harness.scenario().read_system().await.expect("read system");
        let live_group = system.live_group(written.group).expect("live group");
        if !reached.contains(&live_group) {
            reached.push(live_group);
            tracks.push((track, written));
        }
        if tracks.len() as u64 == groups {
            return tracks;
        }
    }
    panic!("the writes reached {} of {groups} groups", tracks.len());
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

/// The nodes that hold a virtual group's spools this epoch, in spool order
async fn owners(harness: &SimnetHarness, group: VirtualGroup) -> Vec<usize> {
    let scenario = harness.scenario();
    let system = scenario.read_system().await.expect("read system");
    let live_group = system.live_group(group).expect("live group");
    let account = scenario
        .read_group(system.current_epoch, live_group)
        .await
        .expect("read group");
    let group_size = usize::from(harness.config().profile.n());

    account.spools[..group_size]
        .iter()
        .map(|spool| {
            (0..harness.nodes().len())
                .find(|&index| Address::from(scenario.node_address(index)) == spool.node)
                .expect("every spool has an owner in this cluster")
        })
        .collect()
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

/// Advance epochs until the network runs `groups` groups, each fully assigned
async fn wait_groups(harness: &SimnetHarness, groups: u64) {
    for _ in 0..GROWTH_EPOCHS {
        let system = harness.scenario().read_system().await.expect("read system");
        if system.live_group_count == groups {
            for group in 0..groups {
                owners(harness, VirtualGroup(group)).await;
            }
            return;
        }
        harness
            .scenario()
            .self_advance_epoch(EPOCH_TIMEOUT)
            .await
            .expect("advance epoch");
    }
    panic!("the network never reached {groups} groups");
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}
