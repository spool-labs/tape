//! Verifies that every stored coded slice is represented in its group's sample set.

use std::time::Duration;

use tape_core::erasure::group_for_spool;
use tape_core::track::data::BlobData;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_store::ops::{SampleOps, SliceOps, SpoolOps, TrackDataOps};

const NODE_COUNT: usize = 20;
const TARGET_GROUPS: u64 = 1;
const SEATED_STAKE: u64 = 1_000;

const STEADY_EPOCH: u64 = 3;

#[test]
fn sample_coverage() {
    run_simnet_test(sample_coverage_inner);
}

async fn sample_coverage_inner() {
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
        scenario
            .stake_many(&all, SEATED_STAKE)
            .await
            .expect("stake nodes");
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
        .wait_nodes_healthy(Duration::from_secs(30))
        .await
        .expect("nodes healthy");
    harness
        .scenario()
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("committee active");

    let payload: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    harness
        .scenario()
        .upload(harness.admin(), &payload, 32)
        .await
        .expect("upload coded track");

    advance_to_epoch(&harness, EpochNumber(STEADY_EPOCH)).await;

    let mut uncovered = Vec::new();
    for index in 0..NODE_COUNT {
        let Some(node) = harness.node(index) else {
            continue;
        };
        let store = node.context().store.clone();

        for (spool, _) in store.iter_all_spools().expect("iter spools") {
            let group = group_for_spool(spool);
            for (track, _) in store.iter_slice_sizes_by_spool(spool).expect("iter slices") {
                let coded = matches!(
                    store.get_track_data(track).expect("track data"),
                    Some(BlobData::Coded(_))
                );
                if coded && store.track_sample(group, track).expect("sample").is_none() {
                    uncovered.push((index, spool, track));
                }
            }
        }
    }

    assert!(
        uncovered.is_empty(),
        "{} held slices have no sample row, first few: {:?}",
        uncovered.len(),
        &uncovered[..uncovered.len().min(5)]
    );
}

async fn advance_to_epoch(harness: &SimnetHarness, target: EpochNumber) {
    let scenario = harness.scenario();
    let timeout = Duration::from_secs(300);
    loop {
        if scenario.current_epoch_number().await.expect("current epoch") >= target.0 {
            return;
        }
        scenario
            .self_advance_epoch(timeout)
            .await
            .expect("advance epoch toward target");
    }
}
