//! Private clusters on small profiles that lose nodes while spares stand by.
//! Nodes only fail here and every count is k.

use std::time::{Duration, Instant};

use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::{ErasureCode, RSParams};
use tape_core::spooler::VirtualGroup;
use tape_core::thresholds::TrustModel;
use tape_core::track::types::CompressedTrack;
use tape_core::types::BasisPoints;
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

const THREE_OF_FOUR: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
const FOUR_OF_SIX: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(4, 2));

const STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 32;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const SLICE_TIMEOUT: Duration = Duration::from_secs(120);
const EPOCH_TIMEOUT: Duration = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

// with a spare in the committee, losing a node stalls nothing: the epoch moves
// on and the survivors rebuild the lost spool between them
#[test]
fn spare_node() {
    run_simnet_test(|| lost_nodes_inner(THREE_OF_FOUR, 1));
}

// a 4-of-6 cluster with two spares loses two owners at once and rebuilds both spools
#[test]
fn two_lost() {
    run_simnet_test(|| lost_nodes_inner(FOUR_OF_SIX, 2));
}

async fn lost_nodes_inner(profile: ErasureCode, lost_count: usize) {
    let node_count = usize::from(profile.n()) + lost_count;
    let mut harness = start_cluster(profile, node_count, 1).await;

    let before = payload(256 * 1024);
    let (first, first_track) = write(&harness, &before).await;
    wait_full_group(&harness, first, &first_track, SLICE_TIMEOUT).await;

    // Owners of the written group, so every lost node takes a slice with it.
    let lost = owners(&harness, first_track.group).await[..lost_count].to_vec();
    harness.stop_nodes(&lost).await.expect("stop nodes");

    let after = payload(300 * 1024);
    let (second, second_track) = write(&harness, &after).await;

    // A lost node may already sit in the next committee, so it can take two
    // epochs before the survivors own every spool.
    wait_full_group(&harness, first, &first_track, EPOCH_TIMEOUT).await;
    wait_full_group(&harness, second, &second_track, EPOCH_TIMEOUT).await;
    let survivors = owners(&harness, first_track.group).await;
    assert!(
        lost.iter().all(|node| !survivors.contains(node)),
        "a stopped node still owns a spool"
    );
    assert_eq!(read(&harness, first).await, before);
    assert_eq!(read(&harness, second).await, after);

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

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}
