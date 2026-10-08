// Challenge rounds on a 20-node cluster. Every round, each node proves a
// sampled piece of a track to its group peers, and each peer's board shows
// what arrived and how long it took.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::spooler::GroupIndex;
use tape_core::types::{BasisPoints, EpochNumber, SlotNumber, SpoolIndex, StorageUnits};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, TAPENET_GROUP_SIZE, run_simnet_test,
};
use tape_node::features::challenge::board::{BoardRound, Outcome, Seen};
use tape_node::features::challenge::holders::{bundle_at, own_spools};
use tape_node::features::challenge::prover::build_batches;
use tape_node::features::challenge::wire::to_req;
use tape_peer_http::HttpApi;
use tape_protocol::api::{Api, ChallengeBoard, CHALLENGE_BOARD_PATH};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;
use tape_store::ops::SliceOps;
use rpc_litesvm::LiteSvmRpc;

// Simnet epochs advance on wall-clock, so the tapes are reserved well past the run.
const EPOCHS: u64 = 40;
// The short tape expires two epochs after it is reserved, and is renewed then.
const SHORT_EPOCHS: u64 = 2;
const CAPACITY_MB: u64 = 8;
const CODED_BYTES: usize = 64 * 1024;
const CODED_TRACKS: usize = 6;
const INLINE_TRACKS: usize = 6;
const ROUNDS: usize = 6;
// The newest rounds can still have proofs on the way.
const UNSETTLED_ROUNDS: usize = 2;
const PRESET_CADENCE: u64 = 5;
const VOTED_CADENCE: u64 = 3;
const FLOOR_CADENCE: u64 = 1;
const TARGET_GROUPS: u64 = 2;
const PEERS: usize = TAPENET_GROUP_SIZE - 1;
const SILENT_NODE: usize = 5;
const SEND_DELAY: Duration = Duration::from_millis(2_000);
const ROUND_TIMEOUT: Duration = Duration::from_secs(120);
const REPAIR_TIMEOUT: Duration = Duration::from_secs(300);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);

type Client = Tapedrive<LiteSvmRpc, HttpApi>;

/// What a set of boards holds for a set of rounds
#[derive(Debug, Default)]
struct Tally {
    ok: usize,
    invalid: usize,
    wrong_track: usize,
    empty: usize,
}

#[test]
fn healthy_cluster() {
    run_simnet_test(healthy_cluster_inner);
}

// with tracks written an epoch earlier, every node's board fills with valid
// proofs from every peer spool, and nothing is invalid or for the wrong track
async fn healthy_cluster_inner() {
    let mut harness = cluster(&[], 1).await;
    let all = everyone();

    advance(&harness).await;
    reserve_and_write(&harness, EPOCHS, "first").await;
    advance(&harness).await;

    let start = epoch_start(&harness).await;
    wait_rounds(&harness, &all, start, ROUNDS).await;

    let rounds = settled_rounds(&harness, &all, start);
    let tally = tally(&rounds);
    println!("healthy: {} settled rounds after slot {}, {tally:?}", rounds.len(), start.0);

    assert!(rounds.len() >= ROUNDS - UNSETTLED_ROUNDS, "too few settled rounds");
    assert_eq!(tally.invalid, 0, "an invalid proof was recorded");
    assert_eq!(tally.wrong_track, 0, "a proof for another track was recorded");
    assert_eq!(tally.empty, 0, "a peer spool sent no proof in a settled round");
    assert_eq!(tally.ok, rounds.len() * all.len() * PEERS);

    harness.stop_all().await.expect("stop runtimes");
}

#[test]
fn churn() {
    run_simnet_test(churn_inner);
}

// deletes, a close, a rewind, a renew and new writes in the current epoch
// never make two nodes pick different tracks
async fn churn_inner() {
    let mut harness = cluster(&[], 1).await;
    let all = everyone();
    let owner = harness.scenario().sdk(harness.admin());

    advance(&harness).await;
    let kept = reserve_and_write(&harness, EPOCHS, "kept").await;
    let closed = reserve_and_write(&harness, EPOCHS, "closed").await;
    let rewound = reserve_and_write(&harness, EPOCHS, "rewound").await;
    let short = reserve_and_write(&harness, SHORT_EPOCHS, "short").await;
    advance(&harness).await;
    advance(&harness).await;

    let start = epoch_start(&harness).await;
    wait_rounds(&harness, &all, start, 2).await;

    for index in 0..CODED_TRACKS / 2 {
        owner
            .delete_object(&kept, &format!("kept/pages/{index}.bin"))
            .await
            .expect("delete a coded track");
        owner
            .delete_object(&kept, &format!("kept/notes/{index}.txt"))
            .await
            .expect("delete an inline track");
    }
    owner.close_tape(&closed).await.expect("close a tape");
    owner.rewind_tape(&rewound).await.expect("rewind a tape");
    owner
        .renew_tape(&short, StorageUnits::mb(CAPACITY_MB), EPOCHS)
        .await
        .expect("renew the expired tape");
    wait_listed(&owner, &rewound, 0).await;
    write_tracks(&harness, &rewound, "again").await;

    wait_rounds(&harness, &all, start, ROUNDS).await;

    let rounds = every_round(&harness, &all, start);
    let tally = tally(&rounds);
    println!("churn: {} rounds after slot {}, {tally:?}", rounds.len(), start.0);

    assert_eq!(tally.wrong_track, 0, "two nodes picked different tracks");
    assert_eq!(tally.invalid, 0, "an invalid proof was recorded");
    assert!(tally.ok > rounds.len() * PEERS, "few proofs arrived");

    harness.stop_all().await.expect("stop runtimes");
}

#[test]
fn stopped_and_wiped() {
    run_simnet_test(stopped_and_wiped_inner);
}

// a stopped node's spool stays empty on every peer and fills again once it is
// back. A node that lost its data stays empty until repair brings it back.
async fn stopped_and_wiped_inner() {
    let mut harness = cluster(&[], 1).await;
    let all = everyone();
    let peers = everyone_but(SILENT_NODE);

    advance(&harness).await;
    reserve_and_write(&harness, EPOCHS, "first").await;
    advance(&harness).await;
    let start = epoch_start(&harness).await;
    wait_rounds(&harness, &all, start, 3).await;
    let spool = spool_of(&harness, SILENT_NODE);

    harness.stop_nodes(&[SILENT_NODE]).await.expect("stop a node");
    let stopped_at = newest_slot(&harness, &peers);
    wait_rounds(&harness, &peers, stopped_at, 4).await;
    let while_stopped = seen_for(&harness, &peers, spool, SlotNumber(stopped_at.0 + PRESET_CADENCE));
    assert!(!while_stopped.is_empty());
    assert!(while_stopped.iter().all(Option::is_none), "a stopped node was seen: {while_stopped:?}");

    harness
        .start_nodes_with_retry(&[SILENT_NODE], 3, Duration::from_millis(200))
        .await
        .expect("start the node again");
    wait_seen(&harness, &peers, spool, ROUND_TIMEOUT).await;

    harness.stop_nodes(&[SILENT_NODE]).await.expect("stop the node");
    harness.wipe_nodes(&[SILENT_NODE]).expect("wipe the node");
    let wiped_at = newest_slot(&harness, &peers);
    harness
        .start_nodes_with_retry(&[SILENT_NODE], 3, Duration::from_millis(200))
        .await
        .expect("start the wiped node");
    let held_at_start = slices_held(&harness, SILENT_NODE);

    wait_seen(&harness, &peers, spool, REPAIR_TIMEOUT).await;

    // Until its data is back the node sends nothing, so an entry is empty or
    // valid and never anything else.
    let since_wipe = seen_for(&harness, &peers, spool, SlotNumber(wiped_at.0 + PRESET_CADENCE));
    let empty = since_wipe.iter().filter(|seen| seen.is_none()).count();
    let slowest = since_wipe.iter().flatten().map(|seen| seen.ms).max();
    let held_now = slices_held(&harness, SILENT_NODE);
    println!(
        "wiped: {held_at_start} slices at start, {held_now} once seen again, {empty} of {} entries empty, slowest {slowest:?} ms",
        since_wipe.len()
    );
    assert_eq!(held_at_start, 0, "the wiped node came up with slices");
    assert!(held_now > 0, "the node was seen again with no slices");
    for seen in since_wipe.iter().flatten() {
        assert_eq!(seen.outcome, Outcome::Ok);
    }

    let rounds = every_round(&harness, &peers, start);
    let tally = tally(&rounds);
    println!("stopped and wiped: {} rounds, {tally:?}", rounds.len());
    assert_eq!(tally.wrong_track, 0);
    assert_eq!(tally.invalid, 0);

    harness.stop_all().await.expect("stop runtimes");
}

#[test]
fn epoch_turn() {
    run_simnet_test(epoch_turn_inner);
}

// across an epoch turn that doubles the groups, the rounds restart from the
// advance slot, the old holders answer under the old spool indexes until the
// epoch turns Active, and the new group's spools appear from there
async fn epoch_turn_inner() {
    let mut harness = cluster(&[], TARGET_GROUPS).await;
    let all = everyone();

    // A round every slot, so the stretch before the epoch turns Active holds several.
    harness
        .scenario()
        .set_challenge_cadence_many(&all, FLOOR_CADENCE)
        .await
        .expect("vote the cadence");
    reserve_and_write(&harness, EPOCHS, "first").await;

    // The voted group count goes live one epoch after the vote is counted.
    advance(&harness).await;
    let epoch = advance(&harness).await;
    let turned = harness.scenario().read_epoch().await.expect("read epoch");
    let start = turned.start_slot;
    assert_eq!(turned.total_groups, TARGET_GROUPS);
    assert_eq!(turned.preferences.challenge_cadence, FLOOR_CADENCE);
    harness
        .scenario()
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("all nodes active");
    wait_answered_by(&harness, &all, start, EpochNumber(epoch), ROUNDS).await;

    let rounds = every_round(&harness, &all, start);
    let (mut before, mut after) = (0, 0);
    for copies in &rounds {
        for round in copies {
            let Some(opened) = round.opened else {
                continue;
            };
            let elapsed = round.slot.0 - start.0;
            assert_eq!(opened.round.number.0, elapsed / FLOOR_CADENCE);
            assert_eq!(opened.round.epoch, EpochNumber(epoch));

            let has_new_group = round
                .entries
                .iter()
                .any(|entry| GroupIndex(1).contains(entry.spool));
            if opened.answering == EpochNumber(epoch) {
                after += 1;
            } else {
                before += 1;
                assert_eq!(opened.answering, EpochNumber(epoch - 1));
                assert!(!has_new_group, "a new group's spool before the epoch turned Active");
            }
        }
    }
    println!("epoch turn: {before} round copies before Active, {after} after");
    assert!(before > 0, "no round before the epoch turned Active");
    assert!(after > 0, "no round after the epoch turned Active");

    // Both the old holders' rounds and the new holders' rounds fill with valid proofs.
    let settled = settled_rounds(&harness, &all, start);
    let tally = tally(&settled);
    let mut new_group_entries = 0;
    let mut by_new_holders = 0;
    for copies in &settled {
        for round in copies {
            if round.opened.is_some_and(|opened| opened.answering == EpochNumber(epoch)) {
                by_new_holders += 1;
                new_group_entries += round
                    .entries
                    .iter()
                    .filter(|entry| GroupIndex(1).contains(entry.spool))
                    .count();
            }
        }
    }
    println!(
        "epoch turn: {} settled rounds, {by_new_holders} copies by the new holders, \
         {new_group_entries} entries for the new group, {tally:?}",
        settled.len()
    );
    assert_eq!(tally.wrong_track, 0);
    assert_eq!(tally.invalid, 0);
    assert_eq!(tally.empty, 0, "a holder sent no proof in a settled round");
    assert!(by_new_holders > 0, "no settled round answered by the new holders");
    assert!(new_group_entries > 0, "the new group's spools never appeared");

    harness.stop_all().await.expect("stop runtimes");
}

#[test]
fn cadence_vote() {
    run_simnet_test(cadence_vote_inner);
}

// the first epoch runs at the preset cadence, and a vote moves the next
// epoch's rounds to the voted one from its start slot
async fn cadence_vote_inner() {
    let mut harness = cluster(&[], 1).await;
    let all = everyone();
    let first = harness.scenario().read_epoch().await.expect("read epoch");
    assert_eq!(first.preferences.challenge_cadence, PRESET_CADENCE);

    reserve_and_write(&harness, EPOCHS, "first").await;
    advance(&harness).await;
    let preset_start = epoch_start(&harness).await;
    harness
        .scenario()
        .set_challenge_cadence_many(&all, VOTED_CADENCE)
        .await
        .expect("vote the cadence");
    wait_rounds(&harness, &all, preset_start, 2).await;

    advance(&harness).await;
    advance(&harness).await;
    let voted = harness.scenario().read_epoch().await.expect("read epoch");
    let voted_start = voted.start_slot;
    assert_eq!(voted.preferences.challenge_cadence, VOTED_CADENCE);
    wait_rounds(&harness, &all, voted_start, ROUNDS).await;

    for copies in every_round(&harness, &all, voted_start) {
        let elapsed = copies[0].slot.0 - voted_start.0;
        assert_eq!(elapsed % VOTED_CADENCE, 0, "a round off the voted schedule");
    }
    let slots: Vec<u64> = every_round(&harness, &all, voted_start)
        .iter()
        .map(|copies| copies[0].slot.0)
        .collect();
    assert!(slots.windows(2).any(|pair| pair[1] - pair[0] == VOTED_CADENCE));

    harness.stop_all().await.expect("stop runtimes");
}

#[test]
fn flag_off_and_late() {
    run_simnet_test(flag_off_and_late_inner);
}

// a node with the flag off is absent on its peers' boards and says so on its
// own. When its proofs are sent late, its peers record the real delay.
async fn flag_off_and_late_inner() {
    let mut harness = cluster(&[SILENT_NODE], 1).await;
    let peers = everyone_but(SILENT_NODE);

    advance(&harness).await;
    reserve_and_write(&harness, EPOCHS, "first").await;
    advance(&harness).await;
    let start = epoch_start(&harness).await;
    wait_rounds(&harness, &peers, start, 4).await;
    let spool = spool_of(&harness, SILENT_NODE);

    let absent = seen_for(&harness, &peers, spool, start);
    assert!(!absent.is_empty());
    assert!(absent.iter().all(Option::is_none), "a node with the flag off was seen");
    let board = fetch_board(&harness, SILENT_NODE).await;
    assert!(!board.is_enabled);
    assert!(board.rounds.is_empty());
    let enabled = fetch_board(&harness, peers[0]).await;
    assert!(enabled.is_enabled);
    assert!(!enabled.rounds.is_empty());

    // The test now sends the silent node's proofs for the newest round itself,
    // a known time after that round opened.
    let silent = harness.node(SILENT_NODE).expect("node").context();
    let state = silent.state();
    let newest = harness.scenario().challenge_rounds(peers[0], start);
    let opened = newest[newest.len() - 1].opened.expect("an opened round");
    let bundle = bundle_at(&state, opened.answering).expect("answering assignment");
    let batches = build_batches(
        &silent.store,
        bundle,
        silent.node_address(),
        opened.round,
        silent.params().profile,
        &HashSet::new(),
    );
    assert_eq!(batches.len(), PEERS);

    tokio::time::sleep(SEND_DELAY).await;
    for (peer, batch) in batches {
        silent
            .api
            .challenge(peer, &to_req(batch))
            .await
            .expect("late proofs accepted");
    }

    let delays = wait_seen_at(&harness, &peers, spool, opened.round.slot).await;
    println!("late: {delays:?}");
    for seen in delays {
        assert_eq!(seen.outcome, Outcome::Ok);
        assert!(seen.ms >= SEND_DELAY.as_millis() as i64, "recorded {} ms", seen.ms);
        assert!(seen.ms < 3 * SEND_DELAY.as_millis() as i64, "recorded {} ms", seen.ms);
    }

    harness.stop_all().await.expect("stop runtimes");
}

fn everyone() -> Vec<usize> {
    (0..TAPENET_GROUP_SIZE).collect()
}

fn everyone_but(node: usize) -> Vec<usize> {
    everyone().into_iter().filter(|index| *index != node).collect()
}

fn epoch_timeout() -> Duration {
    Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5)
}

/// Start 20 nodes voting for `groups` groups, with challenge rounds on for
/// all but `flag_off`, and wait until the first epoch is Active
async fn cluster(flag_off: &[usize], groups: u64) -> SimnetHarness {
    let mut harness = SimnetBuilder::new()
        .node_count(TAPENET_GROUP_SIZE)
        .runtime_mode(NodeRuntimeMode::Full)
        .challenge(true)
        .file_log(true)
        .build()
        .expect("build harness");
    for &node in flag_off {
        harness.node_mut(node).expect("node").set_challenge(false);
    }
    let all = everyone();

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, groups)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(Duration::from_secs(30))
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("all nodes active");

    harness
}

/// Wait for the next epoch and return its number
async fn advance(harness: &SimnetHarness) -> u64 {
    harness
        .scenario()
        .self_advance_epoch(epoch_timeout())
        .await
        .expect("advance the epoch")
}

/// The slot of the block that advanced the chain into its current epoch
async fn epoch_start(harness: &SimnetHarness) -> SlotNumber {
    harness
        .scenario()
        .read_epoch()
        .await
        .expect("read epoch")
        .start_slot
}

/// Reserve a tape and write coded and inline tracks to it
async fn reserve_and_write(harness: &SimnetHarness, epochs: u64, prefix: &str) -> TapeKey {
    let owner: Client = harness.scenario().sdk(harness.admin());
    let tape = TapeKey::generate();
    owner
        .reserve(&tape, StorageUnits::mb(CAPACITY_MB), epochs)
        .await
        .expect("reserve the tape");
    write_tracks(harness, &tape, prefix).await;

    tape
}

/// Write coded and inline tracks to an empty tape, and wait until the nodes list them
async fn write_tracks(harness: &SimnetHarness, tape: &TapeKey, prefix: &str) {
    let owner: Client = harness.scenario().sdk(harness.admin());

    for index in 0..CODED_TRACKS {
        let page = vec![index as u8 + prefix.len() as u8; CODED_BYTES];
        owner
            .put_object(tape, &format!("{prefix}/pages/{index}.bin"), &page, None)
            .await
            .expect("coded write");
    }
    for index in 0..INLINE_TRACKS {
        let note = format!("{prefix} ledger line {index}").into_bytes();
        owner
            .put_object(tape, &format!("{prefix}/notes/{index}.txt"), &note, Some("text/plain"))
            .await
            .expect("inline write");
    }

    wait_listed(&owner, tape, CODED_TRACKS + INLINE_TRACKS).await;
}

/// Wait until the nodes list exactly `expected` tracks on a tape
async fn wait_listed(owner: &Client, tape: &TapeKey, expected: usize) {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    loop {
        let listed = owner
            .list_tracks_by_tape(&tape.address(), None, 64)
            .await
            .map(|(tracks, _)| tracks.len());
        if listed.as_ref().is_ok_and(|count| *count == expected) {
            return;
        }
        assert!(Instant::now() < deadline, "tape lists {listed:?} tracks, expected {expected}");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// The live spool a node holds in the current epoch
fn spool_of(harness: &SimnetHarness, node: usize) -> SpoolIndex {
    let context = harness.node(node).expect("node").context();
    let state = context.state();

    own_spools(&state.current, context.node_address())[0]
}

/// How many slices a node's store holds
fn slices_held(harness: &SimnetHarness, node: usize) -> u64 {
    let context = harness.node(node).expect("node").context();
    let (slices, _) = context.store.slice_totals().expect("slice totals");

    slices
}

/// The newest round slot on any of the nodes' boards
fn newest_slot(harness: &SimnetHarness, nodes: &[usize]) -> SlotNumber {
    let mut newest = SlotNumber(0);
    for &node in nodes {
        for round in harness.scenario().challenge_rounds(node, newest) {
            newest = newest.max(round.slot);
        }
    }
    newest
}

/// Wait until each of `nodes` has opened `count` rounds with entries after `since`
async fn wait_rounds(harness: &SimnetHarness, nodes: &[usize], since: SlotNumber, count: usize) {
    let deadline = Instant::now() + ROUND_TIMEOUT;
    loop {
        let mut fewest = usize::MAX;
        for &node in nodes {
            let opened = harness
                .scenario()
                .challenge_rounds(node, since)
                .iter()
                .filter(|round| round.opened.is_some() && !round.entries.is_empty())
                .count();
            fewest = fewest.min(opened);
        }
        if fewest >= count {
            return;
        }
        assert!(Instant::now() < deadline, "a node opened only {fewest} rounds after slot {}", since.0);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until each of `nodes` has opened `count` rounds after `since` in which
/// the holders of `epoch` answer
async fn wait_answered_by(
    harness: &SimnetHarness,
    nodes: &[usize],
    since: SlotNumber,
    epoch: EpochNumber,
    count: usize,
) {
    let deadline = Instant::now() + ROUND_TIMEOUT;
    loop {
        let mut fewest = usize::MAX;
        for &node in nodes {
            let answered = harness
                .scenario()
                .challenge_rounds(node, since)
                .iter()
                .filter(|round| round.opened.is_some_and(|opened| opened.answering == epoch))
                .count();
            fewest = fewest.min(answered);
        }
        if fewest >= count {
            return;
        }
        assert!(Instant::now() < deadline, "a node opened only {fewest} rounds for {epoch:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Every node's copy of each round after `since`, slot by slot
fn every_round(harness: &SimnetHarness, nodes: &[usize], since: SlotNumber) -> Vec<Vec<BoardRound>> {
    let mut by_slot: BTreeMap<SlotNumber, Vec<BoardRound>> = BTreeMap::new();
    for &node in nodes {
        for round in harness.scenario().challenge_rounds(node, since) {
            by_slot.entry(round.slot).or_default().push(round);
        }
    }

    by_slot.into_values().collect()
}

/// The rounds after `since` that every one of `nodes` opened, leaving out the newest ones
fn settled_rounds(harness: &SimnetHarness, nodes: &[usize], since: SlotNumber) -> Vec<Vec<BoardRound>> {
    let mut settled = Vec::new();
    for copies in every_round(harness, nodes, since) {
        let opened = copies.iter().filter(|round| round.opened.is_some()).count();
        if opened == nodes.len() {
            settled.push(copies);
        }
    }
    settled.truncate(settled.len().saturating_sub(UNSETTLED_ROUNDS));
    settled
}

/// What each of `nodes` saw from one spool in every round it opened from `from` on
fn seen_for(
    harness: &SimnetHarness,
    nodes: &[usize],
    spool: SpoolIndex,
    from: SlotNumber,
) -> Vec<Option<Seen>> {
    let mut seen = Vec::new();
    for &node in nodes {
        for round in harness.scenario().challenge_rounds(node, SlotNumber(from.0 - 1)) {
            for entry in &round.entries {
                if entry.spool == spool {
                    seen.push(entry.seen);
                }
            }
        }
    }
    seen
}

/// Wait until every one of `nodes` records a valid proof from a spool in one round
async fn wait_seen(harness: &SimnetHarness, nodes: &[usize], spool: SpoolIndex, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let from = newest_slot(harness, nodes);
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        let seen = seen_for(harness, nodes, spool, from);
        let valid = seen
            .iter()
            .filter(|seen| seen.is_some_and(|seen| seen.outcome == Outcome::Ok))
            .count();
        if valid >= nodes.len() {
            return;
        }
        assert!(Instant::now() < deadline, "spool {} is still not seen by its peers", spool.0);
    }
}

/// Wait until every one of `nodes` has an entry filled for a spool in the round at `slot`
async fn wait_seen_at(
    harness: &SimnetHarness,
    nodes: &[usize],
    spool: SpoolIndex,
    slot: SlotNumber,
) -> Vec<Seen> {
    let deadline = Instant::now() + ROUND_TIMEOUT;
    loop {
        let mut filled = Vec::new();
        for &node in nodes {
            for round in harness.scenario().challenge_rounds(node, SlotNumber(slot.0 - 1)) {
                if round.slot != slot {
                    continue;
                }
                for entry in &round.entries {
                    if entry.spool == spool {
                        filled.extend(entry.seen);
                    }
                }
            }
        }
        if filled.len() == nodes.len() {
            return filled;
        }
        assert!(Instant::now() < deadline, "only {} peers recorded the late proof", filled.len());
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Poll a node's board route the way the dashboard does, with no client certificate
async fn fetch_board(harness: &SimnetHarness, node: usize) -> ChallengeBoard {
    let target = harness.node(node).expect("node");
    let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
    let client = tape_peer_tls::apply_pinned_tls(builder, target.tls_pubkey())
        .expect("pinned tls")
        .build()
        .expect("http client");

    let response = client
        .get(format!("{}{CHALLENGE_BOARD_PATH}", target.base_url()))
        .send()
        .await
        .expect("board request");
    assert!(response.status().is_success(), "board route answered {}", response.status());
    let body = response.bytes().await.expect("board body");

    wincode::deserialize(&body).expect("board decodes")
}

fn tally(rounds: &[Vec<BoardRound>]) -> Tally {
    let mut tally = Tally::default();
    for copies in rounds {
        for round in copies {
            for entry in &round.entries {
                match entry.seen.map(|seen| seen.outcome) {
                    Some(Outcome::Ok) => tally.ok += 1,
                    Some(Outcome::Invalid) => tally.invalid += 1,
                    Some(Outcome::WrongTrack) => tally.wrong_track += 1,
                    None => tally.empty += 1,
                }
            }
        }
    }
    tally
}
