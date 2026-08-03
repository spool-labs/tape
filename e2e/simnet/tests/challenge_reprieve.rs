//! The run arm's reprieve: a node that stops answering but stays reachable
//! keeps its seat.
//!
//! The eviction rule has two arms and only one of them makes a claim a live
//! probe can answer. A run of consecutive misses says the peer stopped
//! answering; the lifetime rate says it answers badly. This drives a node into
//! the first state and not the second: it banks enough successes that its rate
//! stays above the floor, then misses a run of rounds with its process up and
//! serving, then answers again. Every observer's rule fires on the run alone,
//! every observer probes it, and nobody votes.
//!
//! The corruption is the same instrument `corruption_eviction` uses, restored
//! the moment the run lands. There the misses keep coming until the rate falls
//! through the floor and the node is rightly evicted; here they stop first, and
//! the difference between those two runs is the whole point of judging the arms
//! apart.

use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_core::challenge::PeerRecord;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::columns::SliceCol;
use tape_store::ops::{ChallengeOps, SliceOps, SpoolOps};
use tape_store::types::SliceKey;

// The committee holds exactly the group floor, plus the spare an eviction would
// draw on. It stays unused, which is the assertion.
const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const VICTIM: usize = 1;
const OBSERVER: usize = 0;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;

/// Successes banked before the run starts.
///
/// The rate arm fires below half, so a run of three costs nothing while the
/// peer has answered at least that many. Five leaves room for two more misses
/// to settle between the run landing and the slice going back.
const BANKED_SUCCESSES: u64 = 5;

/// One opportunity settles per epoch or so, so banking is most of the run.
const BANK_TIMEOUT: Duration = Duration::from_secs(600);

/// Three consecutive misses at roughly one round per epoch.
const RUN_TIMEOUT: Duration = Duration::from_secs(420);

/// Long enough that a vote would have landed. `corruption_eviction` gets from a
/// fired rule to a suspension well inside this.
const REPRIEVE_WINDOW: Duration = Duration::from_secs(240);

// a node whose rule fires on the run arm alone, and which answers, keeps its seat
#[test]
fn challenge_reprieve() {
    run_simnet_test(challenge_reprieve_inner);
}

async fn challenge_reprieve_inner() {
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

    // One track big enough to dominate the byte-weighted draw, so every round
    // lands on it and the record moves at the round cadence.
    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let (_, track, _) = harness
        .scenario()
        .upload(harness.admin(), &payload, 32)
        .await
        .expect("upload coded track");

    let victim = node_address(&harness, VICTIM);
    let spool = node_spool(&harness, VICTIM);

    // Bank successes first. Without them the run and the rate fire together and
    // the probe is never consulted, which is the case corruption_eviction covers.
    await_record(&harness, victim, BANK_TIMEOUT, "banked successes", |record| {
        record.successes >= BANKED_SUCCESSES
    })
    .await;

    // Two copies: the stored bytes to put back verbatim, and the decoded slice
    // to check the rot took and then undid. Mutation stays in raw space, since
    // writing a decoded value back through the column kills the node.
    let stored = raw_slice(&harness, spool, track);
    let served = node_slice(&harness, VICTIM, spool, track).expect("victim holds its slice");

    write_raw_slice(&harness, spool, track, &rotted(&stored));
    assert_ne!(
        node_slice(&harness, VICTIM, spool, track).expect("slice still present"),
        served,
        "the corruption did not take"
    );

    // The state the reprieve is defined on: the peer has stopped answering and
    // its lifetime rate is still good, so the rule fires on the run alone.
    let fired = await_record(&harness, victim, RUN_TIMEOUT, "a run-only rule", |record| {
        record.run_fires() && !record.rate_fires()
    })
    .await;

    // Put the bytes back before the misses can carry the rate through the floor.
    write_raw_slice(&harness, spool, track, &stored);
    assert_eq!(
        node_slice(&harness, VICTIM, spool, track).expect("slice still present"),
        served,
        "the slice did not come back"
    );

    // The premise of the reprieve: the peer answers. Nothing here ever stopped
    // its process, which is exactly what separates it from an absent node.
    harness
        .scenario()
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("the victim answers");

    let start = Instant::now();
    while start.elapsed() < REPRIEVE_WINDOW {
        for node in 0..COMMITTEE_NODES {
            let suspended = harness
                .scenario()
                .read_node(node)
                .await
                .expect("read node")
                .suspended_until;
            assert_eq!(
                suspended,
                EpochNumber(0),
                "node {node} was suspended although its rule fired on the run alone \
                 and it answered throughout (record at the fire: {fired:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The end-user view: nothing about the reprieve cost the payload.
    let reread = harness
        .scenario()
        .download(harness.admin(), &track)
        .await
        .expect("download after the reprieve");
    assert_eq!(reread, payload, "download should match the upload");
}

/// Poll one observer's record of the victim until it satisfies `wanted`.
///
/// Reads a group-mate rather than the victim, since a node keeps no record of
/// itself and the vote is cast on what its peers saw.
async fn await_record(
    harness: &SimnetHarness,
    victim: Address,
    timeout: Duration,
    wanted: &str,
    ready: impl Fn(&PeerRecord) -> bool,
) -> PeerRecord {
    let start = Instant::now();
    loop {
        let record = observer_record(harness, victim);
        if ready(&record) {
            return record;
        }
        assert!(
            start.elapsed() < timeout,
            "no {wanted} within {timeout:?}, last record {record:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn observer_record(harness: &SimnetHarness, victim: Address) -> PeerRecord {
    harness
        .node(OBSERVER)
        .expect("observer node")
        .context()
        .store
        .peer_record(victim)
        .expect("peer record")
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

/// The same rot `corruption_eviction` applies: every byte past the header, so
/// one sampled sub-leaf is certain to land on it, and the value still decodes.
fn rotted(slice: &[u8]) -> Vec<u8> {
    let mut bytes = slice.to_vec();
    for byte in &mut bytes[16..] {
        *byte ^= 0xFF;
    }
    bytes
}

/// The victim's stored slice value, exactly as the column holds it.
fn raw_slice(harness: &SimnetHarness, spool: SpoolIndex, track: Address) -> Vec<u8> {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let key = wincode::serialize(&SliceKey::new(spool, track)).expect("slice key");

    store
        .inner()
        .inner()
        .get(SliceCol::CF_NAME, &key)
        .expect("raw get")
        .expect("slice bytes present")
}

/// Write a stored slice value back, leaving the sidecar and the size index
/// exactly as they were.
fn write_raw_slice(harness: &SimnetHarness, spool: SpoolIndex, track: Address, bytes: &[u8]) {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let key = wincode::serialize(&SliceKey::new(spool, track)).expect("slice key");

    store
        .inner()
        .inner()
        .put(SliceCol::CF_NAME, &key, bytes)
        .expect("raw put");
}
