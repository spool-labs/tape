//! A slice the uploader abandons is repaired once its owner comes back.
//!
//! The uploader stops retrying a node the moment quorum accepts, so a node
//! that is down for a write never gets its slice and the write certifies
//! without it, leaving one owner holding nothing for a certified track.
//!
//! Reading the track would prove nothing, because a read needs only k slices
//! and succeeds with the gap still open. The assertion is on the owner's store.

use std::time::{Duration, Instant};

use store::Store;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::ops::{SliceOps, SpoolOps};

// One group, one spool each, so stopping a node is guaranteed to strand a
// slice rather than landing on a spool somebody else also holds.
const NODE_COUNT: usize = GROUP_SIZE;
const VICTIM: usize = 1;
const WITNESS: usize = 2;
const TARGET_GROUPS: u64 = 1;
const STAKE: u64 = 1_000;
const STEADY_EPOCH: u64 = 2;

/// Slack rather than a budget: the repair is queued as soon as the victim
/// replays the certify it missed.
const HEAL_TIMEOUT: Duration = Duration::from_secs(180);

// a slice stranded by an unreachable owner is repaired once that owner returns
#[test]
fn upload_gap_repair() {
    run_simnet_test(upload_gap_repair_inner);
}

async fn upload_gap_repair_inner() {
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

    // Settle ownership before taking a node away, so the spool the victim
    // loses a slice for is the spool it comes back to.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;
    harness
        .scenario()
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at the steady epoch");

    let spool = node_spool(&harness, VICTIM);

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
    assert!(
        node_slice(&harness, VICTIM, spool, track).is_none(),
        "the stopped owner should hold nothing for a track written while it was down"
    );
    let witness = node_slice(&harness, WITNESS, node_spool(&harness, WITNESS), track)
        .expect("a running owner holds its slice");

    harness
        .start_nodes(&[VICTIM])
        .await
        .expect("restart the victim");

    assert_eq!(
        node_spool(&harness, VICTIM),
        spool,
        "the victim came back on a different spool, so the repair is not the thing measured"
    );

    let healed = wait_slice(&harness, VICTIM, spool, track, HEAL_TIMEOUT).await;

    assert_eq!(
        healed.len(),
        witness.len(),
        "the repaired slice is a different length to a peer's"
    );
    assert!(
        !healed.is_empty(),
        "the repaired slice is empty"
    );
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

/// Poll the victim's own store until the missing slice appears.
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
            "spool {spool} never repaired its missing slice within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn node_spool(harness: &SimnetHarness, index: usize) -> SpoolIndex {
    let node = harness.node(index).expect("node");
    let spools = node.context().store.iter_all_spools().expect("iter spools");
    spools.first().copied().map(|(spool, _)| spool).expect("node owns a spool")
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
