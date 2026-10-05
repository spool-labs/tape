//! Spool recovery driven by group growth: a blob lands in the single live
//! group, the live group count then rises to TARGET_GROUPS, the spools spread
//! across the new groups, and whoever owns the blob's group afterwards has to
//! fetch the slices it does not already hold.
//!
//! Ownership only moves when the allocation forces it. The spooler retains
//! incumbents wherever stake allows, so a stable allocation produces no
//! migration, nothing to recover, and a slice check that passes without
//! exercising anything. The ownership assertion below guards against that.
//!
//! The blob has to exist before the perturbation, or the new owner has nothing
//! to fetch. Unlike the committee-expansion driver, this one does not wait on
//! committee seating, which is redrawn every epoch and takes an unpredictable
//! number of them to fill.
//!
//! How much this exercises: 4 to 5 of the group's 20 spools change hands, so
//! that many slices are actually fetched. Churn scales with the group count but
//! plateaus, 5 groups moved one spool and 20 moved four to five, so a full
//! group re-fetch is not reachable this way; that would mean removing the
//! incumbents outright, which nothing here does.
//!
//! The blob also has to outlive the whole wait. A tape is swept the epoch after
//! its term ends, taking the track record and every slice with it, so a lease
//! that runs out mid-test fails here with every owner at zero slices,
//! incumbents included. That reads like a recovery fault and is not one.
//!
//! One simnet per test binary: two of them in one file race for ports.

use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::BasisPoints;
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};

const TARGET_GROUPS: u64 = 20;
const NODE_COUNT: usize = 25;

/// Wall seconds for the raised group count to come live and the spools to
/// spread across the new groups.
const MIGRATION_TIMEOUT_SECS: u64 = 600;

/// Wall seconds for the seating to be redrawn once the groups are live.
/// Seating is redrawn on an epoch boundary, so this only has to span one.
const SEATING_TIMEOUT_SECS: u64 = 200;

/// Wall seconds for one slice-recovery wait. Charged twice, once before the
/// perturbation and once after.
const RECOVERY_TIMEOUT_SECS: u64 = 200;

/// Wall seconds for the fleet to come up and seat the genesis committee.
const STARTUP_TIMEOUT_SECS: u64 = 200;

/// Blob lifetime, in epochs.
///
/// A tape is swept the epoch after its term ends and takes the track record and
/// every slice with it, so a blob that expires before the last check turns this
/// into a test of garbage collection: every owner reads zero slices, migrated or
/// not. Epochs run around a minute here, so ten of them is roughly ten minutes
/// of cover against runs that finish in two or three. Reservations also live in
/// a fixed ring of future epochs and a lease past its end is refused outright
/// with NoCapacity, which a longer lease walks straight into.
const BLOB_EPOCHS: u64 = 10;

/// Spool owners that must change for this to be exercising recovery at all.
/// Seating is redrawn from a per-epoch seed, so the count varies run to run;
/// 4 and 5 of 20 have both been observed. This is a floor against the test
/// going vacuous, not a measurement, so it sits below the observed range
/// rather than on it.
const MIN_MIGRATED_SPOOLS: usize = 3;

#[test]
fn spool_recovery_group_growth() {
    run_simnet_test(inner);
}

async fn inner() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let genesis_committee: Vec<usize> = (0..GROUP_SIZE).collect();
    let all: Vec<usize> = (0..NODE_COUNT).collect();

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(STARTUP_TIMEOUT_SECS);
    let recovery_timeout = Duration::from_secs(RECOVERY_TIMEOUT_SECS);
    let migration_timeout = Duration::from_secs(MIGRATION_TIMEOUT_SECS);
    let seating_timeout = Duration::from_secs(SEATING_TIMEOUT_SECS);
    let scenario = harness.scenario();

    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&genesis_committee, active_timeout)
        .await
        .expect("genesis committee active");
    assert_group_counts(&scenario, 1, 1).await;

    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let (_tape_key, track_address, track) = scenario
        .upload(harness.admin(), &data, BLOB_EPOCHS)
        .await
        .expect("upload blob");

    assert!(track.is_coded(), "uploaded track should be a blob track");
    assert!(track.is_certified(), "uploaded blob track should be certified");

    let before = group_owners(&scenario, track.group).await;
    wait_current_owner_slices(&scenario, &track_address, track.group, recovery_timeout)
        .await
        .expect("first owners store all blob slices");

    // The perturbation. Raising the target pulls more groups live, which
    // reallocates spools away from the nodes currently holding them.
    scenario
        .set_spool_groups_many(&all, TARGET_GROUPS)
        .await
        .expect("raise the target group count");
    wait_live_groups(&scenario, TARGET_GROUPS, migration_timeout)
        .await
        .expect("groups came live");

    wait_migrated_owners(&scenario, track.group, &before, seating_timeout)
        .await
        .expect("spools spread onto the new groups' owners");

    wait_current_owner_slices(&scenario, &track_address, track.group, recovery_timeout)
        .await
        .expect("new owners recovered all blob slices");

    let reread = scenario
        .download(harness.admin(), &track_address)
        .await
        .expect("download blob after migration");
    assert_eq!(reread, data, "post-migration download should match upload");

    harness.stop_all().await.expect("stop runtimes");
}

async fn wait_live_groups(
    scenario: &SimnetScenario<'_>,
    expected: u64,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let system = scenario.read_system().await?;
        if system.live_group_count >= expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "timed out waiting for {expected} live groups, observed {}",
                system.live_group_count
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait for the spools to spread onto the newly live groups' owners.
///
/// The group count going live and the spools being reassigned are two different
/// events: seating is redrawn at an epoch boundary, which lands some time after
/// the allocation change that causes it. Reading the owners once, straight
/// after the groups come live, catches the old seating and reports zero
/// migrated.
async fn wait_migrated_owners(
    scenario: &SimnetScenario<'_>,
    group: GroupIndex,
    before: &[Address],
    timeout: Duration,
) -> Result<Vec<Address>> {
    let start = Instant::now();

    loop {
        let after = group_owners(scenario, group).await;
        let migrated = before.iter().zip(&after).filter(|(a, b)| a != b).count();
        if migrated >= MIN_MIGRATED_SPOOLS {
            return Ok(after);
        }

        if start.elapsed() >= timeout {
            bail!(
                "only {migrated} of {GROUP_SIZE} spool owners changed at {TARGET_GROUPS} groups"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_current_owner_slices(
    scenario: &SimnetScenario<'_>,
    track: &Address,
    group: GroupIndex,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();

    loop {
        let observed = scenario
            .count_current_owner_slices(track, group)
            .await
            .expect("count current owner slices");
        if observed == GROUP_SIZE {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "timed out waiting for current group owners to hold {GROUP_SIZE} slices, observed {observed}"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn group_owners(scenario: &SimnetScenario<'_>, group: GroupIndex) -> Vec<Address> {
    let system = scenario.read_system().await.expect("read system");
    let group = scenario
        .read_group(system.current_epoch, group)
        .await
        .expect("read group");

    group.spools.iter().map(|spool| spool.node).collect()
}

async fn assert_group_counts(
    scenario: &SimnetScenario<'_>,
    expected_target: u64,
    expected_live: u64,
) {
    let system = scenario.read_system().await.expect("read system");

    assert_eq!(
        system.target_group_count, expected_target,
        "unexpected target group count"
    );
    assert_eq!(
        system.live_group_count, expected_live,
        "unexpected live group count"
    );
}
