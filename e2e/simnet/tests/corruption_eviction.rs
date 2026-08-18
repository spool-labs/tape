//! Verifies that challenge detection evicts a corrupt node and preserves readable data.

use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::{GROUP_SIZE, group_for_spool};
use tape_core::types::{BasisPoints, EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::columns::SliceCol;
use tape_core::track::data::BlobData;
use tape_store::ops::{SampleOps, SliceOps, SpoolOps, TrackDataOps};
use tape_store::types::SliceKey;

const COMMITTEE_NODES: usize = GROUP_SIZE;
const NODE_COUNT: usize = COMMITTEE_NODES + 1;
const SPARE_NODE: usize = COMMITTEE_NODES;
const VICTIM: usize = 1;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;
const SPARE_STAKE: u64 = 500;
const STEADY_EPOCH: u64 = 2;

/// Allows three draws of the victim's spool plus proposal and voting.
const EVICT_TIMEOUT: Duration = Duration::from_secs(900);

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

    // One coded track to check the healing against. The run crosses many
    // epochs, so the lease has to outlive any plausible arc.
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

    // Rot every slice the spool holds, not just the uploaded track's. The
    // sample set spans the whole group, so one rotten track is drawn only in
    // proportion to its share of the bytes and detection follows 1/p. This
    // test is about a node serving rotten data being caught, not about how
    // long a small p takes, so it puts p at 1.
    assert!(rot_spool(&harness, spool) > 0, "the victim holds nothing to rot");
    assert_ne!(
        node_slice(&harness, VICTIM, spool, track).expect("slice still present"),
        original,
        "the corruption did not take"
    );

    // From here nothing touches the victim. Its own runtime keeps its seat,
    // answers every round with rotten proofs, and collects the misses.
    let suspended = wait_eviction(&harness, spool, EVICT_TIMEOUT).await;
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

/// Poll for suspension while keeping newly written snapshot slices corrupt.
async fn wait_eviction(
    harness: &SimnetHarness,
    spool: SpoolIndex,
    timeout: Duration,
) -> EpochNumber {
    let start = Instant::now();
    loop {
        rot_spool(harness, spool);

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

fn rot_spool(harness: &SimnetHarness, spool: SpoolIndex) -> usize {
    let node = harness.node(VICTIM).expect("victim node");
    let store = node.context().store.clone();
    let mut rotted = 0usize;

    for (track, _) in store.iter_slice_sizes_by_spool(spool).expect("iter slices") {
        rot_slice(&store, spool, track);
        rotted += 1;
    }

    // Inline tracks answer from the payload rather than a slice, and a group's
    // set holds plenty of them, so leaving them intact lets the victim answer
    // honestly whenever the draw lands on one and the run of misses never builds.
    for (track, _) in store
        .iter_track_samples_by_group(group_for_spool(spool))
        .expect("iter sample rows")
    {
        if let Ok(Some(BlobData::Inline(payload))) = store.get_track_data(track) {
            store
                .put_track_data(track, BlobData::Inline(vec![0x5A; payload.len().max(1)]))
                .expect("rot inline");
            rotted += 1;
        }
    }
    rotted
}

fn rot_slice<Db: Store>(store: &tape_store::TapeStore<Db>, spool: SpoolIndex, track: Address) {
    let raw = store.inner().inner();

    let key = wincode::serialize(&SliceKey::new(spool, track)).expect("slice key");
    let mut bytes = raw
        .get(SliceCol::CF_NAME, &key)
        .expect("raw get")
        .expect("slice bytes present");

    // Overwrite the whole payload, not a patch: a round samples one sub-leaf,
    // so a patch covering a fraction p of the slice is found in 1/p rounds and
    // only p = 1 is caught by the first draw. A fixed pattern rather than a
    // flip keeps this idempotent. The leading bytes stay so the stored value
    // still decodes.
    for byte in &mut bytes[16..] {
        *byte = 0x5A;
    }
    raw.put(SliceCol::CF_NAME, &key, &bytes).expect("raw put");
}
