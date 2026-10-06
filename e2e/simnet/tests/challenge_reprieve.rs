//! Verifies that a reachable node is reprieved when only the consecutive-miss rule fires.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tape_store_api::{Column, Store};
use tape_core::challenge::{EvictionPolicy, PeerRecord};
use tape_core::spooler::{GroupIndex, VirtualGroup};
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, TAPENET_GROUP_SIZE, run_simnet_test,
};
use tape_store::columns::SliceCol;
use tape_core::track::data::BlobData;
use tape_store::ops::{ChallengeOps, SampleOps, SliceOps, TrackDataOps};
use tape_store::types::SliceKey;

// This scenario needs eviction within its test budget. The runtime uses this
// declared policy through the same configuration path as a deployed node.
const EVICTION_POLICY: EvictionPolicy = EvictionPolicy {
    max_consecutive_misses: 6,
    min_opportunities: 60,
    minimum_success_rate_bps: BasisPoints(5_000),
};

const COMMITTEE_NODES: usize = TAPENET_GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const VICTIM: usize = 1;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;

/// Leave room for evidence already in flight while the test restores the bytes.
const RESTORE_MARGIN_ROUNDS: u64 = 2;

/// Bound the setup independently of epoch duration.
const BANK_TIMEOUT: Duration = Duration::from_secs(600);

/// Allows the configured miss streak and phase transitions.
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
        .eviction(true)
        .eviction_policy(EVICTION_POLICY)
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
    let (_, track, written) = harness
        .scenario()
        .upload(harness.admin(), &payload, 32)
        .await
        .expect("upload coded track");

    // Records and rounds go by the victim's live spool. Its slice of the track
    // sits under the track's own virtual group, at the same position.
    let victim = node_address(&harness, VICTIM);
    let spool = node_spool(&harness, VICTIM);
    let track_spool = stored_spool(spool, written.group);

    // Bank successes first. Without them the run and the rate fire together and
    // the probe is never consulted, which is the case corruption_eviction covers.
    // The gate is on the thinnest record in the group, since every member votes
    // on its own and the margin has to hold for all of them.
    await_record(
        &harness,
        victim,
        spool,
        BANK_TIMEOUT,
        "banked successes",
        |record| {
            let mut projected = *record;
            projected.opportunities +=
                EVICTION_POLICY.max_consecutive_misses + RESTORE_MARGIN_ROUNDS;

            projected.success_rate() >= EVICTION_POLICY.minimum_success_rate_bps
        },
    )
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
    let served = node_slice(&harness, VICTIM, track_spool, track).expect("victim holds its slice");
    let mut stored: HashMap<(SpoolIndex, Address), Vec<u8>> = HashMap::new();

    let mut inline: HashMap<Address, Vec<u8>> = HashMap::new();
    let start = Instant::now();
    let fired = loop {
        for (held, bytes) in raw_spool(&harness, spool) {
            if stored.contains_key(&held) {
                continue;
            }
            write_raw_slice(&harness, held.0, held.1, &rotted(&bytes));
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
        let records = observer_records(&harness, victim, spool);
        assert!(
            records.iter().all(|record| !record.rate_fires(&EVICTION_POLICY)),
            "reprieve prerequisite lost: an observer failed the rate rule: {records:?}"
        );

        if let Some(record) = records
            .iter()
            .find(|record| record.run_fires(&EVICTION_POLICY))
        {
            break *record;
        }
        let record = observer_record(&harness, victim, spool);
        assert!(
            start.elapsed() < RUN_TIMEOUT,
            "no run-only rule within {RUN_TIMEOUT:?}, weakest record {record:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    assert!(stored.len() > 1, "expected the spool to hold more than one track");
    assert_ne!(
        node_slice(&harness, VICTIM, track_spool, track).expect("slice still present"),
        served,
        "the corruption did not take"
    );

    // Put the bytes back before the misses can carry the rate through the floor.
    for (held, bytes) in &stored {
        write_raw_slice(&harness, held.0, held.1, bytes);
    }
    for (track, payload) in &inline {
        write_inline(&harness, *track, payload);
    }
    assert_eq!(
        node_slice(&harness, VICTIM, track_spool, track).expect("slice still present"),
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
        let records = observer_records(harness, victim, spool);
        if !records.is_empty() && records.iter().all(&ready) {
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
    observer_records(harness, victim, spool)
        .into_iter()
        .min_by_key(|record| (record.success_rate().0, record.successes))
        .unwrap_or_default()
}

fn observer_records(harness: &SimnetHarness, victim: Address, spool: SpoolIndex) -> Vec<PeerRecord> {
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
        .collect()
}

/// The one live spool a node is assigned.
fn node_spool(harness: &SimnetHarness, index: usize) -> SpoolIndex {
    let context = harness.node(index).expect("node").context();
    let spools = context.state().member_live_spools(context.node_address());
    spools.first().copied().expect("node owns a spool")
}

/// The stored spool a node keeps a track's slice under: the track's virtual
/// group, at the node's position in its live group.
fn stored_spool(live_spool: SpoolIndex, group: VirtualGroup) -> SpoolIndex {
    let position = GroupIndex::containing(live_spool)
        .position_of(live_spool)
        .expect("a position in its group");
    group.spool_at(position)
}

/// The tracks in the sample set of the victim's live spool, each with the
/// stored spool the victim keeps its slice under.
fn sampled_tracks(harness: &SimnetHarness, live_spool: SpoolIndex) -> Vec<(SpoolIndex, Address)> {
    let context = harness.node(VICTIM).expect("victim node").context();
    let fold = context.state().fold().expect("a live group");
    let group = GroupIndex::containing(live_spool);
    let position = group.position_of(live_spool).expect("a position in its group");

    let mut tracks = Vec::new();
    for groups in fold.carried_ranges(group) {
        for (virtual_group, track, _) in context
            .store
            .iter_track_samples_in(groups)
            .expect("iter sample rows")
        {
            tracks.push((virtual_group.spool_at(position), track));
        }
    }
    tracks
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

fn inline_payloads(harness: &SimnetHarness, live_spool: SpoolIndex) -> Vec<(Address, Vec<u8>)> {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();

    sampled_tracks(harness, live_spool)
        .into_iter()
        .filter_map(|(_, track)| match store.get_track_data(track) {
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

/// Every slice the victim answers a round on its live spool with, raw.
///
/// A snapshot chunk sits under the live spool's own index, and every other
/// track under its own virtual group at the same position.
fn raw_spool(harness: &SimnetHarness, live_spool: SpoolIndex) -> Vec<((SpoolIndex, Address), Vec<u8>)> {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let raw = store.inner().inner();

    let mut held: Vec<(SpoolIndex, Address)> = store
        .iter_slice_sizes_by_spool(live_spool)
        .expect("iter slice sizes")
        .into_iter()
        .map(|(track, _)| (live_spool, track))
        .collect();
    for (stored, track) in sampled_tracks(harness, live_spool) {
        if stored != live_spool {
            held.push((stored, track));
        }
    }

    held.into_iter()
        .filter_map(|(stored, track)| {
            let key = wincode::serialize(&SliceKey::new(stored, track)).expect("slice key");
            let bytes = raw.get(SliceCol::CF_NAME, &key).expect("raw get")?;
            Some(((stored, track), bytes.into_vec()))
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
