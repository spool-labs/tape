//! Verifies that an owner restores a slice missed while it was offline, by
//! repair, and by recovery when a holder in its y-section serves a corrupt slice.

use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, TAPENET_GROUP_SIZE, run_simnet_test,
};
use tape_slicer::SliceMetadata;
use tape_store::ops::SliceOps;

// One group, one spool each, so stopping a node is guaranteed to strand a
// slice rather than landing on a spool somebody else also holds.
const NODE_COUNT: usize = TAPENET_GROUP_SIZE;
const VICTIM: usize = 1;
const TARGET_GROUPS: u64 = 1;
const STAKE: u64 = 1_000;
const STEADY_EPOCH: u64 = 2;

/// Slack rather than a budget: the repair is queued as soon as the victim
/// replays the certify it missed.
const HEAL_TIMEOUT: Duration = Duration::from_secs(180);

/// Wait long enough for the uploader to push its slice to every holder
const STRAGGLER_WAIT: Duration = Duration::from_secs(75);

/// A slice the stopped victim missed, and what a peer holds for it
struct Gap {
    live_spool: SpoolIndex,
    spool: SpoolIndex,
    group: VirtualGroup,
    track: Address,
    witness_len: usize,
}

#[test]
fn upload_gap_repair() {
    run_simnet_test(upload_gap_repair_inner);
}

// every repair of the gap asks the corrupt holder, so its leaf check fails and
// recovery restores the slice from the other holders
#[test]
fn corrupt_holder() {
    run_simnet_test(corrupt_holder_inner);
}

async fn upload_gap_repair_inner() {
    let mut harness = start().await;
    let gap = strand(&mut harness).await;
    heal(&mut harness, &gap).await;
}

async fn corrupt_holder_inner() {
    let mut harness = start().await;
    let gap = strand(&mut harness).await;
    corrupt_section_holder(&harness, &gap);
    heal(&mut harness, &gap).await;
}

/// Start the network and settle ownership, so the spool the victim loses a
/// slice for is the spool it comes back to
async fn start() -> SimnetHarness {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
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
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(STAKE).await.expect("stake nodes");
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
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;
    harness
        .scenario()
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at the steady epoch");

    harness
}

/// Stop the victim, write a track it misses, and wait out the uploader's pushes
async fn strand(harness: &mut SimnetHarness) -> Gap {
    let live_spool = node_spool(harness, VICTIM);

    harness
        .stop_nodes(&[VICTIM])
        .await
        .expect("stop the victim before the write");

    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (_, track, written) = harness
        .scenario()
        .upload(harness.admin(), &payload, 32)
        .await
        .expect("write certifies without the stopped owner");

    assert!(written.is_coded(), "the payload must take the coded path");
    assert!(
        written.is_certified(),
        "quorum without the stopped owner should still certify"
    );

    // Read the gap from the store, not through a node that answers for a peer.
    let spool = stored_spool(live_spool, written.group);
    assert!(
        node_slice(harness, VICTIM, spool, track).is_none(),
        "the stopped owner should hold nothing for a track written while it was down"
    );
    let holders = slice_holders(harness, track, written.group);
    let witness_node = harness.node(0).expect("node 0");
    let certify = witness_node.context().params().thresholds.certify;
    assert!(
        holders.len() as u64 >= certify,
        "a certified write left {} of {TAPENET_GROUP_SIZE} owners holding their slice",
        holders.len()
    );
    let (_, witness) = holders
        .into_iter()
        .find(|(index, _)| *index != VICTIM)
        .expect("a holder other than the victim");

    tokio::time::sleep(STRAGGLER_WAIT).await;

    Gap { live_spool, spool, group: written.group, track, witness_len: witness.len() }
}

/// Restart the victim and wait for its slice
async fn heal(harness: &mut SimnetHarness, gap: &Gap) {
    harness
        .start_nodes(&[VICTIM])
        .await
        .expect("restart the victim");

    assert_eq!(
        node_spool(harness, VICTIM),
        gap.live_spool,
        "the victim came back on a different spool, so the repair is not the thing measured"
    );

    let healed = wait_slice(harness, VICTIM, gap.spool, gap.track, HEAL_TIMEOUT).await;

    assert_eq!(
        healed.len(),
        gap.witness_len,
        "the restored slice is a different length to a peer's"
    );
    assert!(
        !healed.is_empty(),
        "the restored slice is empty"
    );
}

/// Flip every payload byte of the slice that a holder in the victim's
/// y-section keeps, leaving its metadata readable
///
/// A same-section helper is in every repair plan for the victim's position,
/// so no repair of the gap can pass its leaf check.
fn corrupt_section_holder(harness: &SimnetHarness, gap: &Gap) {
    let profile = harness.node(0).expect("node 0").context().params().profile;
    let clay = match profile {
        ErasureCode::Clay(clay) => clay,
        other => panic!("the corrupt holder case needs a Clay network, got {other:?}"),
    };
    let q = usize::from(clay.d()) - usize::from(clay.k()) + 1;
    let section = position(gap.live_spool) / q;

    let (index, slice) = slice_holders(harness, gap.track, gap.group)
        .into_iter()
        .find(|(index, _)| *index != VICTIM && position(node_spool(harness, *index)) / q == section)
        .expect("a holder in the victim's y-section");

    let mut corrupt = slice;
    let payload = corrupt.len() - SliceMetadata::SIZE;
    for byte in &mut corrupt[..payload] {
        *byte ^= 0xff;
    }
    let spool = stored_spool(node_spool(harness, index), gap.group);
    harness
        .node(index)
        .expect("holder")
        .context()
        .store
        .put_slice(spool, gap.track, corrupt)
        .expect("corrupt the holder's slice");
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

async fn wait_slice(
    harness: &SimnetHarness,
    index: usize,
    spool: SpoolIndex,
    track: Address,
    timeout: Duration,
) -> Vec<u8> {
    let start = Instant::now();
    loop {
        if let Some(slice) = node_slice(harness, index, spool, track) {
            return slice;
        }
        assert!(
            start.elapsed() < timeout,
            "spool {spool} never restored its missing slice within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The one live spool a node is assigned.
fn node_spool(harness: &SimnetHarness, index: usize) -> SpoolIndex {
    let context = harness.node(index).expect("node").context();
    let spools = context.state().member_live_spools(context.node_address());
    spools.first().copied().expect("node owns a spool")
}

/// A live spool's position in its group
fn position(live_spool: SpoolIndex) -> usize {
    GroupIndex::containing(live_spool)
        .position_of(live_spool)
        .expect("a position in its group")
}

/// The stored spool a node keeps a track's slice under: the track's virtual
/// group, at the node's position in its live group.
fn stored_spool(live_spool: SpoolIndex, group: VirtualGroup) -> SpoolIndex {
    group.spool_at(position(live_spool))
}

/// Owners holding a slice for this track, with the slice each holds.
///
/// Scans every owner rather than naming one: which owners land inside the
/// certifying quorum is not fixed, so a named one is a coin flip.
fn slice_holders(
    harness: &SimnetHarness,
    track: Address,
    group: VirtualGroup,
) -> Vec<(usize, Vec<u8>)> {
    (0..NODE_COUNT)
        .filter_map(|index| {
            let spool = stored_spool(node_spool(harness, index), group);
            let slice = node_slice(harness, index, spool, track)?;
            Some((index, slice))
        })
        .collect()
}

fn node_slice(
    harness: &SimnetHarness,
    index: usize,
    spool: SpoolIndex,
    track: Address,
) -> Option<Vec<u8>> {
    let node = harness.node(index).expect("node");
    node.context().store.get_slice(spool, track).expect("get_slice")
}
