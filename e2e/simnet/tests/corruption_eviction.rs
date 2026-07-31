//! The detect-only contract, end to end: whirlwind detects, the next epoch
//! moves the data, and the reads outlive the corrupt node.
//!
//! Bytes inside one node's stored slice are flipped, sidecar and size index
//! left intact, so every self-detection path stays blind: the scan sees the
//! slice present and the victim keeps answering rounds with proofs built from
//! rotten bytes. Nothing repairs it mid-epoch any more. The group refuses the
//! proofs, the misses accumulate into an eviction, the boundary reassigns the
//! spool, and the successor's sync refuses the rotten slice against the
//! commitment and rebuilds it from the group. The victim runs its own
//! lifecycle throughout: this is the seated node that believes it is whole,
//! is reachable, and is wrong.

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

// The committee holds exactly the group floor, plus a spare to backfill the
// seat the eviction frees.
const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const VICTIM: usize = 1;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;
const STEADY_EPOCH: u64 = 2;

/// Three consecutive misses accumulate across roughly two epochs, and the
/// proposal and votes need another one or two to land.
const EVICT_TIMEOUT: Duration = Duration::from_secs(600);

/// After the boundary the successor has to sync the spool, refuse the rotten
/// slice against the commitment, and rebuild it from the group.
const HEAL_TIMEOUT: Duration = Duration::from_secs(420);

#[test]
fn corruption_eviction() {
    run_simnet_test(corruption_eviction_inner);
}

async fn corruption_eviction_inner() {
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

    // A coded track big enough to dominate the byte-weighted draw, so the
    // first miss almost surely names it. The run crosses many epochs, so the
    // lease has to outlive any plausible arc.
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (_, track, _) = harness
        .scenario()
        .upload(harness.admin(), &payload, 32)
        .await
        .expect("upload coded track");

    // Let rounds run against a full group first, so the corruption lands in
    // the middle of an already-live challenge cadence.
    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH), epoch_timeout).await;

    let spool = node_spool(&harness, VICTIM);
    let original = node_slice(&harness, VICTIM, spool, track).expect("victim holds its slice");

    rot_slice(&harness, spool, track);
    assert_ne!(
        node_slice(&harness, VICTIM, spool, track).expect("slice still present"),
        original,
        "the corruption did not take"
    );

    // From here nothing touches the victim. Its own runtime keeps its seat,
    // answers every round with rotten proofs, and collects the misses.
    let suspended = wait_eviction(&harness, EVICT_TIMEOUT).await;
    assert_ne!(
        suspended,
        EpochNumber(0),
        "the corrupt node was never suspended, so detection did not reach a proposal"
    );

    // One rotten spool must not poison the group.
    for node in (0..COMMITTEE_NODES).filter(|node| *node != VICTIM) {
        let other = harness
            .scenario()
            .read_node(node)
            .await
            .expect("read node")
            .suspended_until;
        assert_eq!(
            other,
            EpochNumber(0),
            "node {node} was suspended although it answered honestly"
        );
    }

    // The next epoch moves the data, without the malicious node: the spool is
    // reassigned, and the successor's sync refuses the rotten slice against
    // the commitment and rebuilds it from the group.
    advance_to_epoch(&harness, suspended, epoch_timeout).await;

    let victim_address = node_address(&harness, VICTIM);
    let start = Instant::now();
    loop {
        if let Some(successor) = spool_successor(&harness, spool, victim_address) {
            if node_slice(&harness, successor, spool, track).as_deref()
                == Some(original.as_slice())
            {
                break;
            }
        }
        assert!(
            start.elapsed() < HEAL_TIMEOUT,
            "no successor served the true bytes within {HEAL_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The victim's own copy stays rotten: detection owes it nothing.
    assert_ne!(
        node_slice(&harness, VICTIM, spool, track).expect("victim copy still present"),
        original,
        "something healed the corrupt node although nothing should"
    );

    // The end-user view: the blob reads back byte-identical.
    let reread = harness
        .scenario()
        .download(harness.admin(), &track)
        .await
        .expect("download after the eviction");
    assert_eq!(reread, payload, "download should match the upload");
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

/// Poll for the suspension. The victim needs no crank: its own runtime keeps
/// the seat, which is exactly the adversary this test is about.
async fn wait_eviction(harness: &SimnetHarness, timeout: Duration) -> EpochNumber {
    let start = Instant::now();
    loop {
        let suspended = harness
            .scenario()
            .read_node(VICTIM)
            .await
            .expect("read the corrupt node")
            .suspended_until;
        if suspended != EpochNumber(0) {
            return suspended;
        }
        assert!(
            start.elapsed() < timeout,
            "no eviction landed within {timeout:?}"
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

fn node_address(harness: &SimnetHarness, index: usize) -> Address {
    harness.node(index).expect("node").context().node_address()
}

/// The live node currently seated on the spool, resolved from any honest
/// node's ingested state, excluding the victim.
fn spool_successor(
    harness: &SimnetHarness,
    spool: SpoolIndex,
    victim: Address,
) -> Option<usize> {
    let observer = harness.node(0).expect("observer node");
    let owner = observer.context().state().spool_owner(spool)?;
    if owner == victim {
        return None;
    }
    (0..NODE_COUNT).find(|index| {
        harness
            .node(*index)
            .map(|node| node.context().node_address() == owner)
            .unwrap_or(false)
    })
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
