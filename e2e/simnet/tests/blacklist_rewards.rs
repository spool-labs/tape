use std::time::{Duration, Instant};

use tape_api::program::EPOCH_DURATION;
use tape_api::program::tapedrive::{blacklist_pda, track_pda};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::{BlacklistEntry, Member};
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::{Address, hash};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::{TrackDataOps, TrackOps};

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const BLACKLIST_NODE: usize = 0;
const DATA_EPOCH: EpochNumber = EpochNumber(2);
const ASSIGNED_EPOCH: EpochNumber = EpochNumber(4);
const CLAIM_EPOCH: EpochNumber = EpochNumber(5);

#[test]
fn baseline() {
    run_simnet_test(|| run(false));
}

#[test]
fn blacklist() {
    run_simnet_test(|| run(true));
}

async fn run(with_blacklist: bool) {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(0)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(EPOCH_DURATION as u64 * 5);

    start(&mut harness, &all, health_timeout, active_timeout, epoch_timeout).await;

    let (track, track_size) = write_data(&harness, with_blacklist).await;
    wait_track(&harness, track, false, active_timeout).await;

    if with_blacklist {
        let scenario = harness.scenario();
        let node = Address::from(scenario.node_address(BLACKLIST_NODE));
        let blacklist = blacklist_pda(node).0;
        let entry_track = track_pda(blacklist, TrackNumber(0)).0;

        wait_track(&harness, entry_track, true, active_timeout).await;
    }

    {
        let scenario = harness.scenario();
        scenario
            .advance_to_epoch(ASSIGNED_EPOCH.0, &all, epoch_timeout)
            .await
            .expect("advance to assigned epoch");

        assert_weights(&scenario, with_blacklist, track_size).await;

        scenario
            .advance_to_epoch(CLAIM_EPOCH.0, &all, epoch_timeout)
            .await
            .expect("advance to claim epoch");
    }

    harness.stop_nodes(&all).await.expect("stop node runtimes");

    {
        let scenario = harness.scenario();
        let members = scenario
            .read_committee(ASSIGNED_EPOCH)
            .await
            .expect("read assigned committee");
        let epoch = scenario
            .read_epoch_at(ASSIGNED_EPOCH)
            .await
            .expect("read assigned epoch");
        let archive = scenario.read_archive().await.expect("read archive");
        let expected = expected_paid(&members, epoch.total_assigned, archive.rewards_pool);

        assert!(expected > TAPE::zero(), "expected non-zero rewards");

        scenario.pool_many(&all).await.expect("advance all pools");

        let archive = scenario.read_archive().await.expect("read archive after pool");
        assert_eq!(
            archive.rewards_paid, expected,
            "paid rewards should match finalized committee weights"
        );

        if with_blacklist {
            assert!(
                archive.rewards_paid < archive.rewards_pool,
                "blacklisted reward weight should stay unpaid"
            );
        }
    }

    harness.stop_all().await.expect("stop harness");
}

async fn start(
    harness: &mut SimnetHarness,
    all: &[usize],
    health_timeout: Duration,
    active_timeout: Duration,
    epoch_timeout: Duration,
) {
    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(all, TARGET_GROUPS)
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
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(all, active_timeout)
        .await
        .expect("all nodes active");
    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to data epoch");
    assert_eq!(
        scenario.current_epoch_number().await.expect("read epoch"),
        DATA_EPOCH.0,
        "unexpected data epoch"
    );
}

async fn write_data(harness: &SimnetHarness, with_blacklist: bool) -> (Address, StorageUnits) {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let tape = tape_key.address();
    let data = vec![0x42; 1024];

    sdk.reserve(&tape_key, StorageUnits::mb(1), 8)
        .await
        .expect("reserve data tape");

    let track = sdk
        .write_raw(&tape_key, hash::hash(b"blacklist-rewards"), &data)
        .await
        .expect("write raw track");
    assert_eq!(track.tape, tape, "raw track tape mismatch");
    assert_eq!(track.group, GroupIndex(0), "unexpected raw track group");

    let track_address = track_pda(track.tape, track.track_number).0;

    if with_blacklist {
        scenario
            .create_blacklist(BLACKLIST_NODE, 4, EpochNumber(10))
            .await
            .expect("create blacklist");
        scenario
            .add_to_blacklist(BLACKLIST_NODE, BlacklistEntry::track(track_address))
            .await
            .expect("add blacklist entry");
    }

    (track_address, track.size)
}

async fn assert_weights(
    scenario: &SimnetScenario<'_>,
    with_blacklist: bool,
    track_size: StorageUnits,
) {
    let system = scenario.read_system().await.expect("read system");
    assert_eq!(
        system.current_epoch, ASSIGNED_EPOCH,
        "unexpected current epoch"
    );

    let group = scenario
        .read_group(ASSIGNED_EPOCH, GroupIndex(0))
        .await
        .expect("read assigned group");
    let node = Address::from(scenario.node_address(BLACKLIST_NODE));
    assert!(
        group.spools.iter().any(|spool| spool.node == node),
        "blacklist node should still own a spool"
    );

    let members = scenario
        .read_committee(ASSIGNED_EPOCH)
        .await
        .expect("read assigned committee");
    assert_eq!(members.len(), NODE_COUNT, "unexpected committee size");

    let member = members
        .iter()
        .find(|member| member.node == node)
        .expect("blacklist node is in committee");
    assert!(member.assigned > StorageUnits::zero(), "assigned weight missing");

    if with_blacklist {
        assert_eq!(
            member.blacklisted, track_size,
            "blacklisted track should reduce member reward weight"
        );
    } else {
        assert!(
            members
                .iter()
                .all(|member| member.blacklisted == StorageUnits::zero()),
            "baseline should not contain blacklisted weight"
        );
    }
}

fn expected_paid(
    members: &[Member],
    total_assigned: StorageUnits,
    rewards_pool: TAPE,
) -> TAPE {
    if total_assigned == StorageUnits::zero() {
        return TAPE::zero();
    }

    let paid = members.iter().fold(0u128, |paid, member| {
        let weight = member
            .assigned
            .checked_sub(member.blacklisted)
            .expect("member blacklisted weight exceeds assigned weight");
        paid + rewards_pool.as_u128() * weight.as_u128() / total_assigned.as_u128()
    });

    TAPE(u64::try_from(paid).expect("expected rewards overflow"))
}

async fn wait_track(
    harness: &SimnetHarness,
    track: Address,
    data: bool,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let seen = harness
            .nodes()
            .iter()
            .filter(|node| node.is_running())
            .filter(|node| {
                let store = &node.context().store;
                let has_track = store.has_track(track).expect("read track");
                let has_data = !data || store.has_track_data(track).expect("read track data");
                has_track && has_data
            })
            .count();

        if seen == harness.nodes().len() {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for track {track} on all nodes, seen {seen}/{}",
            harness.nodes().len()
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
