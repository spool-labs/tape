//! A silently corrupted slice comes back without its owner ever noticing.
//!
//! Bytes inside one node's stored slice are flipped, sidecar and size index
//! left intact, so every self-detection path stays blind: the scan sees the
//! slice present, and the victim keeps answering rounds with proofs built from
//! rotten bytes. The group refuses those answers against the committed leaf,
//! the round settles as a miss, one observer elects itself splicer off the
//! entropy block, reconstructs the slice from the group, and pushes it back
//! through the verified put_slice route, overwriting the rot. Nobody calls
//! anything on the victim's behalf.
//!
//! This is the failure only the challenge can catch: a node that believes it
//! is whole, is reachable, and is wrong.

use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::columns::SliceCol;
use tape_store::ops::{SliceOps, SpoolOps};
use tape_store::types::SliceKey;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const STEADY_EPOCH: u64 = 2;
const VICTIM: usize = 1;

/// A round has to sample the rotten track for the victim's spool, the refusals
/// have to settle as a miss, and the elected splicer has to reconstruct and
/// push, all at the harness's natural round cadence.
const RESTORE_TIMEOUT: Duration = Duration::from_secs(420);

#[test]
fn group_splice() {
    run_simnet_test(group_splice_inner);
}

async fn group_splice_inner() {
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
        scenario
            .stake_many(&all, SEATED_STAKE)
            .await
            .expect("stake nodes");
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
        .expect("nodes active");

    // A coded track big enough to dominate the byte-weighted draw, so the
    // first miss almost surely names it.
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (_, track, _) = harness
        .scenario()
        .upload(harness.admin(), &payload, 8)
        .await
        .expect("upload coded track");

    // Let rounds run against a full group first, so the corruption lands in
    // the middle of an already-live challenge cadence.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;

    let spool = victim_spool(&harness);
    let original = victim_slice(&harness, spool, track).expect("victim holds its slice");

    rot_slice(&harness, spool, track);
    assert_ne!(
        victim_slice(&harness, spool, track).expect("slice still present"),
        original,
        "the corruption did not take"
    );

    // From here nothing touches the victim: it answers with rotten proofs, the
    // group refuses them, and the elected splicer pushes the true bytes back.
    let start = Instant::now();
    loop {
        if victim_slice(&harness, spool, track).as_deref() == Some(original.as_slice()) {
            break;
        }
        assert!(
            start.elapsed() < RESTORE_TIMEOUT,
            "the rotten slice was never spliced back within {RESTORE_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The heal must land before the miss run reaches the eviction rule: a
    // recovering node keeps its seat.
    let suspended = harness
        .scenario()
        .read_node(VICTIM)
        .await
        .expect("read victim")
        .suspended_until;
    assert_eq!(
        suspended,
        EpochNumber(0),
        "the victim was suspended although the splice healed it"
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

fn victim_spool(harness: &SimnetHarness) -> SpoolIndex {
    let node = harness.node(VICTIM).expect("victim node");
    let spools = node.context().store.iter_all_spools().expect("iter spools");
    spools.first().copied().map(|(spool, _)| spool).expect("victim owns a spool")
}

fn victim_slice(harness: &SimnetHarness, spool: SpoolIndex, track: Address) -> Option<Vec<u8>> {
    let node = harness.node(VICTIM).expect("victim node");
    node.context().store.get_slice(spool, track).expect("get_slice")
}

/// Flip bytes in the middle of the stored slice, straight through the raw
/// column so the sidecar and size index stay exactly as they were.
fn rot_slice(harness: &SimnetHarness, spool: SpoolIndex, track: Address) {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let raw = store.inner().inner();

    let key = wincode::serialize(&SliceKey::new(spool, track)).expect("slice key");
    let mut bytes = raw
        .get(SliceCol::CF_NAME, &key)
        .expect("raw get")
        .expect("slice bytes present");

    // Rot the whole payload, not a patch: a round samples one sub-leaf, so a
    // small patch is found in 1/f rounds and f must be 1 for the first draw
    // to hit. The leading bytes stay so the stored value still decodes.
    for byte in &mut bytes[16..] {
        *byte ^= 0xFF;
    }
    raw.put(SliceCol::CF_NAME, &key, &bytes).expect("raw put");
}
