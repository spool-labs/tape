//! Doubling the spool group count splits every group in two, and halving it
//! merges them back.
//!
//! The network runs GROUPS groups and takes blobs until each of the groups a
//! doubling will produce holds one. The count is then voted up to
//! SPLIT_GROUPS. A track keeps the virtual group it was stamped with, and at
//! the higher count half of each group's virtual groups belong to one new
//! group. That new group starts on the nodes of the group it was split from,
//! position by position, so the data is already where it has to be: every
//! holder has its slices the moment the count comes live, and nothing is
//! copied. Each group's bytes divide between it and the group split from it.
//! The count is then voted back down and the earlier layout returns.
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
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, TAPENET_GROUP_SIZE,
    run_simnet_test,
};

const GROUPS: u64 = 4;
const SPLIT_GROUPS: u64 = 8;
const NODE_COUNT: usize = 20;

/// Wall seconds for a voted group count to come live, two epoch boundaries on.
const RESIZE_TIMEOUT_SECS: u64 = 600;

/// Blob lifetime, in epochs. The test runs about a dozen, and a tape is swept
/// the epoch after its term ends.
const BLOB_EPOCHS: u64 = 32;

/// Try this many uploads to land a blob in each of the SPLIT_GROUPS groups to
/// be. The program stamps a track from a slot hash, so each try is a draw.
const MAX_UPLOADS: usize = 96;

/// A group's size counts tracks certified before the epoch that voted it, so
/// a blob shows in the group sizes two epochs after it lands.
const EPOCHS_UNTIL_COUNTED: u64 = 2;

struct Blob {
    address: Address,
    track: CompressedTrack,
    data: Vec<u8>,
}

#[test]
fn group_split() {
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
            .set_spool_groups_many(&all, GROUPS)
            .await
            .expect("vote the starting group count");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(120);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let resize_timeout = Duration::from_secs(RESIZE_TIMEOUT_SECS);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    wait_live_groups(&scenario, GROUPS, resize_timeout)
        .await
        .expect("the starting count came live");
    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("starting epoch active");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active at the starting count");

    let blobs = upload_everywhere(&harness).await;
    for _ in 0..EPOCHS_UNTIL_COUNTED {
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance until the blobs count");
    }

    let nodes_before = group_nodes(&scenario, GROUPS).await;
    let sizes_before = group_sizes(&scenario, GROUPS).await;
    for (group, size) in sizes_before.iter().enumerate() {
        assert!(!size.is_zero(), "group {group} holds blobs before the split");
    }

    // The split.
    scenario
        .set_spool_groups_many(&all, SPLIT_GROUPS)
        .await
        .expect("vote the doubled group count");
    wait_live_groups(&scenario, SPLIT_GROUPS, resize_timeout)
        .await
        .expect("the doubled count came live");

    // Every holder has its slices already. A node of a new group held the
    // same data last epoch, under the group it was split from.
    for blob in &blobs {
        let held = scenario
            .count_current_owner_slices(&blob.address, blob.track.group)
            .await
            .expect("count slices at the split");
        assert_eq!(
            held, TAPENET_GROUP_SIZE,
            "a holder was missing a slice the moment the count doubled"
        );
    }

    let system = scenario.read_system().await.expect("read system");
    for blob in &blobs {
        assert_eq!(
            system.live_group(blob.track.group).expect("live group"),
            live_at(SPLIT_GROUPS, blob.track.group),
        );
    }

    let nodes_split = group_nodes(&scenario, SPLIT_GROUPS).await;
    let sizes_split = group_sizes(&scenario, SPLIT_GROUPS).await;
    for group in 0..SPLIT_GROUPS as usize {
        // A group that was live keeps its nodes, and a new one starts on the
        // nodes of the group that carried it.
        let source = live_at(GROUPS, VirtualGroup(group as u64)).as_usize();
        assert_eq!(
            nodes_split[group], nodes_before[source],
            "group {group} sits on the nodes of group {source}, position by position"
        );
        assert!(!sizes_split[group].is_zero(), "group {group} holds blobs after the split");
    }
    for group in 0..GROUPS as usize {
        // The one new group whose virtual groups this group carried before.
        let taker = (GROUPS as usize..SPLIT_GROUPS as usize)
            .find(|new| live_at(GROUPS, VirtualGroup(*new as u64)).as_usize() == group)
            .expect("each group splits into itself and one new group");
        assert_eq!(
            sizes_before[group].0,
            sizes_split[group].0 + sizes_split[taker].0,
            "group {group}'s bytes divide between it and group {taker}"
        );
    }

    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("split epoch active");
    read_all(&harness, &blobs, "after the split").await;

    // A write after the split lands in one of the groups and reads back.
    let fresh: Vec<u8> = (0..32 * 1024).map(|i| (i % 241) as u8).collect();
    let (_tape_key, fresh_address, fresh_track) = scenario
        .upload(harness.admin(), &fresh, BLOB_EPOCHS)
        .await
        .expect("upload after the split");
    assert!(fresh_track.is_certified());
    let read = scenario
        .download(harness.admin(), &fresh_address)
        .await
        .expect("read the fresh blob");
    assert_eq!(read, fresh);

    // The merge.
    scenario
        .set_spool_groups_many(&all, GROUPS)
        .await
        .expect("vote the starting group count again");
    wait_live_groups(&scenario, GROUPS, resize_timeout)
        .await
        .expect("the starting count came live again");

    let system = scenario.read_system().await.expect("read system");
    for blob in &blobs {
        assert_eq!(
            system.live_group(blob.track.group).expect("live group"),
            live_at(GROUPS, blob.track.group),
            "a blob is back in the group it was written to"
        );
        let held = scenario
            .count_current_owner_slices(&blob.address, blob.track.group)
            .await
            .expect("count slices at the merge");
        assert_eq!(
            held, TAPENET_GROUP_SIZE,
            "a holder was missing a slice the moment the groups merged"
        );
    }
    assert_eq!(
        group_nodes(&scenario, GROUPS).await,
        nodes_before,
        "the groups are back on the nodes they started on"
    );

    scenario
        .wait_phase("Active", epoch_timeout)
        .await
        .expect("merged epoch active");
    read_all(&harness, &blobs, "after the merge").await;

    harness.stop_all().await.expect("stop runtimes");
}

/// Upload blobs until each group of the doubled count would hold one.
async fn upload_everywhere(harness: &SimnetHarness) -> Vec<Blob> {
    let scenario = harness.scenario();
    let mut blobs = Vec::new();
    let mut is_covered = vec![false; SPLIT_GROUPS as usize];

    for attempt in 0..MAX_UPLOADS {
        let data: Vec<u8> = (0..64 * 1024).map(|i| ((i + attempt) % 251) as u8).collect();
        let (_tape_key, address, track) = scenario
            .upload(harness.admin(), &data, BLOB_EPOCHS)
            .await
            .expect("upload blob");
        assert!(track.is_coded(), "uploaded track should be a blob track");
        assert!(track.is_certified(), "uploaded blob track should be certified");

        is_covered[live_at(SPLIT_GROUPS, track.group).as_usize()] = true;
        blobs.push(Blob { address, track, data });

        if is_covered.iter().all(|is_covered| *is_covered) {
            return blobs;
        }
    }

    panic!("{MAX_UPLOADS} uploads never landed in all {SPLIT_GROUPS} groups to be");
}

async fn read_all(harness: &SimnetHarness, blobs: &[Blob], when: &str) {
    for blob in blobs {
        let read = harness
            .scenario()
            .download(harness.admin(), &blob.address)
            .await
            .unwrap_or_else(|error| panic!("read {when}: {error}"));
        assert_eq!(read, blob.data, "a blob read back different {when}");
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

/// The live group that carries a virtual group while `count` groups are live.
fn live_at(count: u64, group: VirtualGroup) -> GroupIndex {
    GroupFold::new(count).expect("live groups").live_group(group)
}

/// The nodes of each of this epoch's first `count` groups, by position.
async fn group_nodes(scenario: &SimnetScenario<'_>, count: u64) -> Vec<Vec<Address>> {
    let system = scenario.read_system().await.expect("read system");
    let groups = scenario
        .read_groups(system.current_epoch, count)
        .await
        .expect("read groups");

    groups
        .iter()
        .map(|group| {
            group
                .spools
                .iter()
                .take(TAPENET_GROUP_SIZE)
                .map(|spool| spool.node)
                .collect()
        })
        .collect()
}

/// The bytes on one spool of each of this epoch's first `count` groups.
async fn group_sizes(scenario: &SimnetScenario<'_>, count: u64) -> Vec<StorageUnits> {
    let system = scenario.read_system().await.expect("read system");
    let groups = scenario
        .read_groups(system.current_epoch, count)
        .await
        .expect("read groups");

    groups.iter().map(|group| group.size).collect()
}
