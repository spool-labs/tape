//! New spool owners whose previous owners are gone get every slice from repair.
//!
//! Two late nodes out-stake the two lowest genesis nodes and take their seats
//! and spools. The two bumped nodes stop before the turn, so the new owners'
//! sync reaches nobody and queues everything for repair. The other eighteen
//! members still hold slices, more than Clay repair's sixteen helpers, and
//! they clear every group threshold, which tops out at fourteen.
//!
//! One simnet per test binary: two of them in one file race for ports.

use std::time::{Duration, Instant};

use tape_api::instruction::MAX_INLINE_WRITE;
use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::spooler::VirtualGroup;
use tape_core::system::EpochPhase;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, TAPENET_GROUP_SIZE,
    run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::TrackDataOps;

const BUMPED: [usize; 2] = [0, 1];
const LATE: [usize; 2] = [TAPENET_GROUP_SIZE, TAPENET_GROUP_SIZE + 1];
const NODE_COUNT: usize = TAPENET_GROUP_SIZE + LATE.len();

const SEATED_STAKE: u64 = 1_000;
const BUMPED_STAKE: u64 = 500;
const LATE_STAKE: u64 = 3_000;

const CODED_SIZES: [usize; 4] = [64 * 1024, 96 * 1024, 128 * 1024, 256 * 1024];

/// A tape is swept the epoch after its term ends, so the lease outlives the test
const LEASE_EPOCHS: u64 = 32;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(200);
const SLICE_TIMEOUT: Duration = Duration::from_secs(200);
const EPOCH_TIMEOUT: Duration = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
const POLL: Duration = Duration::from_millis(100);

type Written = (CompressedTrack, Vec<u8>);

// new owners get every slice and inline payload when their previous owners are gone
#[test]
fn dead_prev_owner() {
    run_simnet_test(inner);
}

async fn inner() {
    let mut harness = start().await;
    let bumped = addresses(&harness, &BUMPED);
    let late = addresses(&harness, &LATE);

    let (coded, inline) = write_tracks(&harness).await;
    {
        let scenario = harness.scenario();
        for (track, _) in &coded {
            wait_full_group(&scenario, track).await;
        }

        // Stake lands two epochs on, so the late nodes join after the genesis
        // owners hold every slice
        scenario.pool_many(&LATE).await.expect("advance late pools");
        scenario
            .stake_many(&LATE, LATE_STAKE)
            .await
            .expect("stake late nodes");
    }

    let closing = wait_handover(&harness.scenario(), &late, &bumped).await;
    // Before the turn, so no sync request reaches them: the new owners sync
    // within milliseconds of it, and a later stop could pass without repair
    harness.stop_nodes(&BUMPED).await.expect("stop bumped nodes");

    let scenario = harness.scenario();
    let group = coded[0].0.group;
    let before = owners(&scenario, closing, group).await;
    let turned = EpochNumber(
        scenario
            .wait_epoch_change(closing.0, EPOCH_TIMEOUT)
            .await
            .expect("epoch turns"),
    );
    let after = owners(&scenario, turned, group).await;

    let mut moved = 0;
    for (position, (old, new)) in before.iter().zip(&after).enumerate() {
        if old == new {
            continue;
        }
        assert!(
            bumped.contains(old) && late.contains(new),
            "position {position} moved from {old} to {new}"
        );
        moved += 1;
    }
    assert!(moved > 0, "no spool moved to a late node");

    for (track, _) in &coded {
        wait_full_group(&scenario, track).await;
    }
    for &node in &LATE {
        wait_inline(&harness, node, &inline).await;
    }
    for (track, data) in coded.iter().chain([&inline]) {
        let read = scenario
            .download(harness.admin(), &track.address())
            .await
            .expect("read the track back");
        assert_eq!(&read, data, "track {} read back changed", track.address());
    }

    wait_active(&scenario, turned).await;
    scenario
        .wait_epoch_change(turned.0, EPOCH_TIMEOUT)
        .await
        .expect("epoch moves on after the turn");

    harness.stop_all().await.expect("stop runtimes");
}

/// Start every node with the genesis committee seated and the late nodes unstaked
async fn start() -> SimnetHarness {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let genesis: Vec<usize> = (0..TAPENET_GROUP_SIZE).collect();
    let seated: Vec<usize> = genesis
        .iter()
        .copied()
        .filter(|node| !BUMPED.contains(node))
        .collect();

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario
            .stake_many(&seated, SEATED_STAKE)
            .await
            .expect("stake seated nodes");
        scenario
            .stake_many(&BUMPED, BUMPED_STAKE)
            .await
            .expect("stake bumped nodes");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(HEALTH_TIMEOUT)
            .await
            .expect("nodes healthy");
        scenario
            .wait_nodes_active(&genesis, ACTIVE_TIMEOUT)
            .await
            .expect("genesis committee active");
    }

    harness
}

/// Write the coded tracks and one inline track to one tape, each certified
async fn write_tracks(harness: &SimnetHarness) -> (Vec<Written>, Written) {
    let sdk = harness.scenario().sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let total = CODED_SIZES.iter().sum::<usize>() + MAX_INLINE_WRITE;
    let capacity = StorageUnits::from_bytes(total as u64) + StorageUnits::mb(2);
    sdk.reserve(&tape_key, capacity, LEASE_EPOCHS)
        .await
        .expect("reserve tape");

    let mut coded = Vec::new();
    for (seed, &size) in CODED_SIZES.iter().enumerate() {
        let data = payload(size, seed);
        let track = sdk
            .write_track(&tape_key, &data)
            .await
            .expect("write coded track");
        assert!(track.is_coded(), "the payload must take the coded path");
        assert!(track.is_certified(), "the coded track should certify");
        coded.push((track, data));
    }

    let raw = payload(MAX_INLINE_WRITE, CODED_SIZES.len());
    let track = sdk
        .write_raw(&tape_key, &raw)
        .await
        .expect("write inline track");
    assert!(track.is_inline(), "the raw write should be inline");
    assert!(track.is_certified(), "the inline track should certify");

    (coded, (track, raw))
}

/// Wait for an epoch whose next committee seats the late nodes and drops the
/// bumped ones, and for it to reach Closing, when that committee is fixed
async fn wait_handover(
    scenario: &SimnetScenario<'_>,
    late: &[Address],
    bumped: &[Address],
) -> EpochNumber {
    let deadline = Instant::now() + EPOCH_TIMEOUT;
    loop {
        let epoch = scenario.read_epoch().await.expect("read epoch");
        if epoch.state.phase() == Some(EpochPhase::Closing) {
            let next = scenario
                .read_committee(epoch.id.next())
                .await
                .expect("read next committee");
            let is_seated = |node: &Address| next.iter().any(|member| member.node == *node);
            if late.iter().all(is_seated) && !bumped.iter().any(is_seated) {
                return epoch.id;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the late nodes never took the bumped nodes' seats"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// The nodes on each position of the group that carries `group` in `epoch`
async fn owners(
    scenario: &SimnetScenario<'_>,
    epoch: EpochNumber,
    group: VirtualGroup,
) -> Vec<Address> {
    let system = scenario.read_system().await.expect("read system");
    let live_group = system.live_group(group).expect("live group");
    let account = scenario
        .read_group(epoch, live_group)
        .await
        .expect("read group");

    account.spools[..TAPENET_GROUP_SIZE]
        .iter()
        .map(|spool| spool.node)
        .collect()
}

/// Wait until every current owner of the track's group holds its slice
async fn wait_full_group(scenario: &SimnetScenario<'_>, track: &CompressedTrack) {
    let deadline = Instant::now() + SLICE_TIMEOUT;
    loop {
        let held = scenario
            .count_current_owner_slices(&track.address(), track.group)
            .await
            .expect("count slices");
        if held == TAPENET_GROUP_SIZE {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{held} of {TAPENET_GROUP_SIZE} owners hold a slice of {}",
            track.address()
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the node holds the inline track's payload
async fn wait_inline(harness: &SimnetHarness, node: usize, inline: &Written) {
    let (track, raw) = inline;
    let ctx = harness.node(node).expect("node").context();
    let expected = Some(BlobData::Inline(raw.clone()));
    let deadline = Instant::now() + SLICE_TIMEOUT;
    loop {
        let held = ctx
            .store
            .get_track_data(track.address())
            .expect("read track data");
        if held == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} lacks the inline payload of {}",
            track.address()
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until the epoch has reached Active
async fn wait_active(scenario: &SimnetScenario<'_>, epoch: EpochNumber) {
    let deadline = Instant::now() + EPOCH_TIMEOUT;
    loop {
        let account = scenario.read_epoch_at(epoch).await.expect("read epoch");
        if account.state.phase() >= Some(EpochPhase::Active) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "epoch {} never reached Active",
            epoch.0
        );
        tokio::time::sleep(POLL).await;
    }
}

fn addresses(harness: &SimnetHarness, nodes: &[usize]) -> Vec<Address> {
    let scenario = harness.scenario();
    nodes
        .iter()
        .map(|&node| Address::from(scenario.node_address(node)))
        .collect()
}

fn payload(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|i| ((i + seed) % 251) as u8).collect()
}
