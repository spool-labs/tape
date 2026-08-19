//! Verifies that a reachable node is reprieved when only the consecutive-miss rule fires.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_core::challenge::PeerRecord;
use tape_core::erasure::{GROUP_SIZE, group_for_spool};
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::columns::SliceCol;
use tape_core::track::data::BlobData;
use tape_store::ops::{ChallengeOps, SampleOps, SliceOps, SpoolOps, TrackDataOps};
use tape_store::types::SliceKey;

const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const VICTIM: usize = 1;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;

/// Keeps the lifetime rate above its floor while the consecutive-miss rule fires.
const BANKED_SUCCESSES: u64 = 5;

/// One opportunity settles per epoch or so, so banking is most of the run.
const BANK_TIMEOUT: Duration = Duration::from_secs(600);

/// Three consecutive misses at roughly one round per epoch.
const RUN_TIMEOUT: Duration = Duration::from_secs(900);

/// Long enough that a vote would have landed. `corruption_eviction` gets from a
/// fired rule to a suspension well inside this.
const REPRIEVE_WINDOW: Duration = Duration::from_secs(240);

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
    // The gate is on the thinnest record in the group, since every member votes
    // on its own and the margin has to hold for all of them.
    await_record(&harness, victim, spool, BANK_TIMEOUT, "banked successes", |record| {
        record.successes >= BANKED_SUCCESSES
    })
    .await;

    // Rot the whole spool and keep rotting it. One pass is not enough: every
    // epoch's snapshot writes fresh tracks into the same spool, and a draw
    // weighted by bytes landing on one of those answers cleanly and resets the
    // run. Re-rotting each pass keeps the misses consecutive whatever the draw
    // picks, and remembering the first sight of each track is what allows all of
    // it to be put back.
    //
    // Mutation stays in raw space, since writing a decoded value back through
    // the column kills the node. `served` is the decoded slice of the uploaded
    // track, kept only to check the rot took and then undid.
    let served = node_slice(&harness, VICTIM, spool, track).expect("victim holds its slice");
    let mut stored: HashMap<Address, Vec<u8>> = HashMap::new();

    let mut inline: HashMap<Address, Vec<u8>> = HashMap::new();
    let start = Instant::now();
    let fired = loop {
        for (held, bytes) in raw_spool(&harness, spool) {
            if stored.contains_key(&held) {
                continue;
            }
            write_raw_slice(&harness, spool, held, &rotted(&bytes));
            stored.insert(held, bytes);
        }
        // Inline tracks answer from the payload, not a slice, and the group's
        // set is full of them. Left alone they hand the victim honest answers
        // and the run of misses never builds.
        for (track, payload) in inline_payloads(&harness, spool) {
            if inline.contains_key(&track) {
                continue;
            }
            write_inline(&harness, track, &vec![0x5A; payload.len().max(1)]);
            inline.insert(track, payload);
        }

        // The state the reprieve is defined on: the peer has stopped answering
        // and its lifetime rate is still good, so the rule fires on the run alone.
        let record = observer_record(&harness, victim, spool);
        if record.run_fires() && !record.rate_fires() {
            break record;
        }
        assert!(
            start.elapsed() < RUN_TIMEOUT,
            "no run-only rule within {RUN_TIMEOUT:?}, weakest record {record:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    assert!(stored.len() > 1, "expected the spool to hold more than one track");
    assert_ne!(
        node_slice(&harness, VICTIM, spool, track).expect("slice still present"),
        served,
        "the corruption did not take"
    );

    // Put the bytes back before the misses can carry the rate through the floor.
    for (held, bytes) in &stored {
        write_raw_slice(&harness, spool, *held, bytes);
    }
    for (track, payload) in &inline {
        write_inline(&harness, *track, payload);
    }
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

async fn await_record(
    harness: &SimnetHarness,
    victim: Address,
    spool: SpoolIndex,
    timeout: Duration,
    wanted: &str,
    ready: impl Fn(&PeerRecord) -> bool,
) -> PeerRecord {
    let start = Instant::now();
    loop {
        let record = observer_record(harness, victim, spool);
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

/// Return the weakest record any group-mate holds for the victim.
fn observer_record(harness: &SimnetHarness, victim: Address, spool: SpoolIndex) -> PeerRecord {
    // The record for the spool this test actually corrupted. Records are per
    // spool, so reading the weakest of the victim's would answer about one the
    // rot never touched.
    (0..COMMITTEE_NODES)
        .filter(|node| *node != VICTIM)
        .map(|node| {
            harness
                .node(node)
                .expect("observer node")
                .context()
                .store
                .peer_record(victim, spool)
                .expect("peer record")
        })
        .min_by_key(|record| (record.successes, record.success_rate().0))
        .unwrap_or_default()
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

fn rotted(slice: &[u8]) -> Vec<u8> {
    let mut bytes = slice.to_vec();
    // Snapshot slices are far smaller than an uploaded track's. One too short
    // to keep a header is rotted whole, since a slice left intact would answer
    // a draw and break the run.
    let head = if bytes.len() > 16 { 16 } else { 0 };
    for byte in &mut bytes[head..] {
        *byte ^= 0xFF;
    }
    bytes
}

fn inline_payloads(harness: &SimnetHarness, spool: SpoolIndex) -> Vec<(Address, Vec<u8>)> {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();

    store
        .iter_track_samples_by_group(group_for_spool(spool))
        .expect("iter sample rows")
        .into_iter()
        .filter_map(|(track, _)| match store.get_track_data(track) {
            Ok(Some(BlobData::Inline(payload))) => Some((track, payload)),
            _ => None,
        })
        .collect()
}

fn write_inline(harness: &SimnetHarness, track: Address, payload: &[u8]) {
    let node = harness.node(VICTIM).expect("victim node");
    node.context()
        .store
        .put_track_data(track, BlobData::Inline(payload.to_vec()))
        .expect("write inline");
}

fn raw_spool(harness: &SimnetHarness, spool: SpoolIndex) -> Vec<(Address, Vec<u8>)> {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let raw = store.inner().inner();

    store
        .iter_slice_sizes_by_spool(spool)
        .expect("iter slice sizes")
        .into_iter()
        .filter_map(|(track, _)| {
            let key = wincode::serialize(&SliceKey::new(spool, track)).expect("slice key");
            let bytes = raw.get(SliceCol::CF_NAME, &key).expect("raw get")?;
            Some((track, bytes.into_vec()))
        })
        .collect()
}

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
