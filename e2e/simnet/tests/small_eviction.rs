//! Eviction on private clusters with small profiles, where nodes only fail and every count is k.

use std::time::{Duration, Instant};

use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::{ErasureCode, RSParams, RepParams};
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::thresholds::TrustModel;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};

const THREE_OF_FOUR: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));
const MIRROR: ErasureCode = ErasureCode::Replication(RepParams::new(2));

const STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 32;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const SLICE_TIMEOUT: Duration = Duration::from_secs(120);
const EVICT_TIMEOUT: Duration = Duration::from_secs(90);
const EPOCH_TIMEOUT: Duration = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

// the other three owners vote a node out, the spare takes its spool, and the
// node returns after its suspension
#[test]
fn eviction() {
    run_simnet_test(|| eviction_inner(THREE_OF_FOUR));
}

// on a 2-copy mirror the vote is one signature, from the owner that isn't accused
#[test]
fn mirror_eviction() {
    run_simnet_test(|| eviction_inner(MIRROR));
}

async fn eviction_inner(profile: ErasureCode) {
    let group_size = usize::from(profile.n());
    let node_count = group_size + 1;
    let mut harness = start(cluster(profile, node_count), 1).await;

    let data = payload(256 * 1024);
    let (track, written) = write(&harness, &data).await;
    wait_full_group(&harness, track, &written, SLICE_TIMEOUT).await;

    // An owner of the group, so the other owners carry the vote alone.
    let target = owners(&harness, written.group).await[0];
    let address = Address::from(harness.scenario().node_address(target));
    let suspended = wait_eviction(&harness, target).await;

    advance_to_epoch(&harness, suspended).await;
    let members = harness
        .scenario()
        .read_committee(suspended)
        .await
        .expect("read the committee");
    assert!(
        members.iter().all(|member| member.node != address),
        "the evicted node is still seated"
    );
    assert_eq!(members.len(), group_size, "the other nodes fill the committee");
    assert!(
        !owners(&harness, written.group).await.contains(&target),
        "the evicted node still owns a spool"
    );

    // The node that took the spool rebuilds the slice.
    wait_full_group(&harness, track, &written, EPOCH_TIMEOUT).await;
    assert_eq!(read(&harness, track).await, data);

    let rejoin = suspended.next();
    advance_to_epoch(&harness, rejoin).await;
    harness
        .scenario()
        .wait_nodes_active(&[target], ACTIVE_TIMEOUT)
        .await
        .expect("the evicted node is active again");
    let members = harness
        .scenario()
        .read_committee(rejoin)
        .await
        .expect("read the committee");
    assert!(
        members.iter().any(|member| member.node == address),
        "the node did not take a seat after its suspension"
    );
    assert_eq!(read(&harness, track).await, data);

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

async fn advance_to_epoch(harness: &SimnetHarness, target: EpochNumber) {
    let scenario = harness.scenario();
    loop {
        if scenario.current_epoch_number().await.expect("current epoch") >= target.0 {
            return;
        }
        scenario
            .self_advance_epoch(EPOCH_TIMEOUT)
            .await
            .expect("advance epoch toward target");
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

/// Propose the eviction and cast the group's vote by hand until it lands, and
/// return the epoch the node is suspended through
async fn wait_eviction(harness: &SimnetHarness, target: usize) -> EpochNumber {
    let deadline = Instant::now() + EVICT_TIMEOUT;
    loop {
        // A proposal expires with its epoch, and a repeat inside one epoch is refused.
        let _ = harness.scenario().propose_eviction(target).await;
        let _ = harness.scenario().vote_eviction(target, GroupIndex(0)).await;

        let suspended = harness
            .scenario()
            .read_node(target)
            .await
            .expect("read the node")
            .suspended_until;
        if suspended != EpochNumber(0) {
            return suspended;
        }
        assert!(Instant::now() < deadline, "the eviction never landed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}
