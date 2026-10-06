//! The spool group count going down and back up with data in place.
//!
//! The network grows to GROWN_GROUPS, takes a blob in a group that will stay
//! live and one in a group that will retire, then votes the count down to
//! SHRUNK_GROUPS. A track keeps the virtual group it was stamped with, so the
//! retired blob has to be served by the live group that carries it: its new
//! holders pull the slices from the nodes that held them, and a read at the
//! turn reaches those old holders while they do. The group that stays live
//! keeps its nodes. The count then goes back up, and the returning group
//! starts on the nodes that already hold its data.
//!
//! A vote reaches the target at the next advance and the live count one epoch
//! after that, so each change of count spans two epoch boundaries.
//!
//! One simnet per test binary: two of them in one file race for ports.

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::track::types::CompressedTrack;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, TAPENET_GROUP_SIZE,
    run_simnet_test,
};

const GROWN_GROUPS: u64 = 4;
const SHRUNK_GROUPS: u64 = 2;
const NODE_COUNT: usize = 20;

/// Wall seconds for a voted group count to come live, two epoch boundaries on.
const RESIZE_TIMEOUT_SECS: u64 = 600;

/// Wall seconds for a group's holders to hold every slice of a blob.
const RECOVERY_TIMEOUT_SECS: u64 = 300;

/// Blob lifetime, in epochs. The test runs about eight, and a tape is swept
/// the epoch after its term ends.
const BLOB_EPOCHS: u64 = 16;

/// Try this many uploads to land one blob in a kept group and one in a retired
/// group. The program picks the group from a slot hash, so each try is a coin flip.
const MAX_UPLOADS: usize = 24;

struct Blob {
    address: Address,
    track: CompressedTrack,
    data: Vec<u8>,
}

#[test]
fn group_decrease() {
    run_simnet_test(inner);
}

async fn inner() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, GROWN_GROUPS)
            .await
            .expect("vote the grown group count");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let resize_timeout = Duration::from_secs(RESIZE_TIMEOUT_SECS);
    let recovery_timeout = Duration::from_secs(RECOVERY_TIMEOUT_SECS);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    // Genesis runs one group. The vote reaches the target at epoch 2 and the
    // live count at epoch 3.
    wait_live_groups(&scenario, GROWN_GROUPS, resize_timeout)
        .await
        .expect("grown groups came live");
    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("grown epoch active");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at the grown count");

    let (kept, retired) = upload_both(&harness).await;
    for blob in [&kept, &retired] {
        wait_holder_slices(&scenario, blob, recovery_timeout)
            .await
            .expect("first holders store every slice");
    }
    let kept_before = holders(&scenario, kept.track.group).await;

    // The shrink.
    scenario
        .set_spool_groups_many(&all, SHRUNK_GROUPS)
        .await
        .expect("vote the lower group count");
    wait_live_groups(&scenario, SHRUNK_GROUPS, resize_timeout)
        .await
        .expect("the lower count came live");

    let system = scenario.read_system().await.expect("read system");
    let retired_group = live_at(GROWN_GROUPS, retired.track.group);
    assert_eq!(system.previous_group_count, GROWN_GROUPS);
    assert_eq!(
        system.live_group(retired.track.group).expect("live group").0,
        retired_group.0 - SHRUNK_GROUPS,
        "at half the count each retired group folds onto the group two below it"
    );
    assert!(
        scenario
            .read_group(system.current_epoch, GroupIndex(GROWN_GROUPS - 1))
            .await
            .is_err(),
        "a retired group has no account in the new epoch"
    );

    // A read at the turn. The new holders may still be pulling, and the nodes
    // that held the group last epoch keep serving it.
    let at_turn = scenario
        .download(harness.admin(), &retired.address)
        .await
        .expect("read the retired group's blob at the turn");
    assert_eq!(at_turn, retired.data);

    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("shrunk epoch active");
    wait_holder_slices(&scenario, &retired, recovery_timeout)
        .await
        .expect("the carrying group's holders pulled every slice");
    wait_holder_slices(&scenario, &kept, recovery_timeout)
        .await
        .expect("the kept group's holders hold every slice");

    let kept_after = holders(&scenario, kept.track.group).await;
    assert_eq!(
        unchanged(&kept_before, &kept_after),
        TAPENET_GROUP_SIZE,
        "a group that stays live keeps its nodes through a shrink"
    );

    for blob in [&kept, &retired] {
        let read = scenario
            .download(harness.admin(), &blob.address)
            .await
            .expect("read after the shrink");
        assert_eq!(read, blob.data);
    }

    // A write after the shrink lands in a live group and reads back.
    let fresh: Vec<u8> = (0..32 * 1024).map(|i| (i % 241) as u8).collect();
    let (_tape_key, fresh_address, fresh_track) = scenario
        .upload(harness.admin(), &fresh, BLOB_EPOCHS)
        .await
        .expect("upload after the shrink");
    assert!(fresh_track.is_certified());
    let read = scenario
        .download(harness.admin(), &fresh_address)
        .await
        .expect("read the fresh blob");
    assert_eq!(read, fresh);

    // The regrow. The retired group returns with data, so it starts on the
    // nodes that already hold that data.
    let carried_by = holders(&scenario, retired.track.group).await;
    scenario
        .set_spool_groups_many(&all, GROWN_GROUPS)
        .await
        .expect("vote the grown group count again");
    wait_live_groups(&scenario, GROWN_GROUPS, resize_timeout)
        .await
        .expect("the grown count came live again");

    let system = scenario.read_system().await.expect("read system");
    assert_eq!(
        system.live_group(retired.track.group).expect("live group"),
        retired_group,
        "the blob is back in the group it was written to"
    );

    let returned = holders(&scenario, retired.track.group).await;
    assert_eq!(
        unchanged(&carried_by, &returned),
        TAPENET_GROUP_SIZE,
        "a group that returns with data starts on the nodes that hold it"
    );

    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("regrown epoch active");
    for blob in [&kept, &retired] {
        wait_holder_slices(&scenario, blob, recovery_timeout)
            .await
            .expect("holders hold every slice after the regrow");
        let read = scenario
            .download(harness.admin(), &blob.address)
            .await
            .expect("read after the regrow");
        assert_eq!(read, blob.data);
    }

    harness.stop_all().await.expect("stop runtimes");
}

/// Upload blobs until one sits in a group that stays live and one in a group that retires.
async fn upload_both(harness: &SimnetHarness) -> (Blob, Blob) {
    let scenario = harness.scenario();
    let mut kept = None;
    let mut retired = None;

    for attempt in 0..MAX_UPLOADS {
        let data: Vec<u8> = (0..64 * 1024).map(|i| ((i + attempt) % 251) as u8).collect();
        let (_tape_key, address, track) = scenario
            .upload(harness.admin(), &data, BLOB_EPOCHS)
            .await
            .expect("upload blob");
        assert!(track.is_coded(), "uploaded track should be a blob track");
        assert!(track.is_certified(), "uploaded blob track should be certified");

        let blob = Blob { address, track, data };
        match live_at(GROWN_GROUPS, blob.track.group).0 < SHRUNK_GROUPS {
            true => kept = kept.or(Some(blob)),
            false => retired = retired.or(Some(blob)),
        }

        if let (Some(_), Some(_)) = (&kept, &retired) {
            break;
        }
    }

    match (kept, retired) {
        (Some(kept), Some(retired)) => (kept, retired),
        _ => panic!("{MAX_UPLOADS} uploads never landed in both a kept and a retired group"),
    }
}

async fn wait_live_groups(
    scenario: &SimnetScenario<'_>,
    expected: u64,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let system = scenario.read_system().await?;
        if system.live_group_count == expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "timed out waiting for {expected} live groups, observed {} with target {}",
                system.live_group_count,
                system.target_group_count
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until the nodes of the live group that carries a blob's group hold all its slices.
async fn wait_holder_slices(
    scenario: &SimnetScenario<'_>,
    blob: &Blob,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let observed = scenario
            .count_current_owner_slices(&blob.address, blob.track.group)
            .await?;
        if observed == TAPENET_GROUP_SIZE {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "timed out waiting for virtual group {} holders to hold {TAPENET_GROUP_SIZE} slices, observed {observed}",
                blob.track.group
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The live group that carries a virtual group while `count` groups are live.
fn live_at(count: u64, group: VirtualGroup) -> GroupIndex {
    GroupFold::new(count).expect("live groups").live_group(group)
}

/// The nodes that hold a virtual group this epoch, by position.
async fn holders(scenario: &SimnetScenario<'_>, group: VirtualGroup) -> Vec<Address> {
    let system = scenario.read_system().await.expect("read system");
    let live_group = system.live_group(group).expect("live group");
    let account = scenario
        .read_group(system.current_epoch, live_group)
        .await
        .expect("read group");

    account.spools.iter().map(|spool| spool.node).collect()
}

/// Count the positions held by the same node in both lists.
fn unchanged(before: &[Address], after: &[Address]) -> usize {
    before
        .iter()
        .zip(after)
        .take(TAPENET_GROUP_SIZE)
        .filter(|(before, after)| before == after)
        .count()
}
