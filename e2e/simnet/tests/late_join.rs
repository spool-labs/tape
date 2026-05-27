use std::time::Duration;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_crypto::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};
use tape_store::ops::{MetaOps, ObjectInfoOps, TrackOps};

const INITIAL_NODES: usize = GROUP_SIZE;
const COMMITTEE_SIZE: u64 = INITIAL_NODES as u64;
const TARGET_GROUPS: u64 = 5;

#[test]
fn late_join() {
    run_simnet_test(late_join_inner);
}

async fn late_join_inner() {
    let initial_nodes: Vec<usize> = (0..INITIAL_NODES).collect();
    let mut harness = SimnetBuilder::new()
        .node_count(INITIAL_NODES)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register initial nodes");
        scenario.stake_all(1_000).await.expect("stake initial nodes");
        scenario
            .set_spool_groups_many(&initial_nodes, TARGET_GROUPS)
            .await
            .expect("set initial spool group preferences");
        scenario
            .set_committee_size_many(&initial_nodes, COMMITTEE_SIZE)
            .await
            .expect("set initial committee size preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start initial runtimes");

    let replay_track = {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .expect("initial nodes healthy");
        scenario
            .wait_nodes_active(&initial_nodes, active_timeout)
            .await
            .expect("initial nodes active");

        while scenario
            .current_epoch_number()
            .await
            .expect("current epoch")
            < 3
        {
            scenario
                .self_advance_epoch(epoch_timeout)
                .await
                .expect("advance epoch before replay write");
        }
        scenario
            .wait_phase("Active", active_timeout)
            .await
            .expect("epoch 3 active");

        let replay_data = vec![0xA5; 64 * 1024];
        let (_, track_address, track) = scenario
            .upload(
                harness.admin(),
                hash::hash(b"late-join-replay-track"),
                &replay_data,
                6,
            )
            .await
            .expect("upload replay track");
        assert!(
            track.is_certified(),
            "replay track should be certified before snapshot"
        );

        while scenario
            .current_epoch_number()
            .await
            .expect("current epoch") < 5
        {
            scenario
                .self_advance_epoch(epoch_timeout)
                .await
                .expect("advance epoch");
        }
        scenario
            .wait_phase("Active", active_timeout)
            .await
            .expect("epoch 5 active");
        assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
        scenario
            .wait_next_quorum(GROUP_SIZE, active_timeout)
            .await
            .expect("epoch 6 committee account ready");

        (track_address, track)
    };

    let late_node = harness.add_node().expect("add late node");

    {
        let scenario = harness.scenario();
        scenario
            .register_many(&[late_node], BasisPoints(100))
            .await
            .expect("register late node");
        scenario
            .stake_many(&[late_node], 1_000)
            .await
            .expect("stake late node");
        scenario
            .set_spool_groups(late_node, TARGET_GROUPS)
            .await
            .expect("set late node spool group preference");
        scenario
            .set_committee_size(late_node, COMMITTEE_SIZE)
            .await
            .expect("set late node committee size preference");
    }

    let prune_slot = harness
        .chain()
        .current_slot()
        .await
        .expect("current slot before pruning blocks");
    let dropped = harness
        .chain()
        .drop_blocks_through(prune_slot)
        .expect("drop rpc blocks before late bootstrap");
    assert!(
        dropped > 0,
        "late bootstrap test should remove historical rpc blocks"
    );

    harness
        .start_nodes_with_retry(&[late_node], 3, Duration::from_millis(200))
        .await
        .expect("start late node");

    {
        let scenario = harness.scenario();
        scenario
            .wait_node_healthy(late_node, health_timeout)
            .await
            .expect("late node healthy");
    }

    let late = harness.node(late_node).expect("late node exists");
    let ctx = late.context();
    let cursor = ctx
        .store
        .get_bootstrap_target_epoch()
        .expect("read bootstrap target")
        .expect("late node should have replayed snapshots");
    let current = ctx.state().epoch();
    let lower = current.saturating_sub(EpochNumber(2));
    let upper = current.prev();

    assert!(
        cursor >= lower && cursor <= upper,
        "bootstrap cursor {cursor} should be the newest finalized snapshot for current epoch {current}"
    );

    let track = ctx
        .store
        .get_track(replay_track.0)
        .expect("read replayed track")
        .expect("late node should have replayed uploaded track metadata");
    assert_eq!(track.tape, replay_track.1.tape, "replayed track tape mismatch");
    assert_eq!(
        track.track_number, replay_track.1.track_number,
        "replayed track number mismatch"
    );
    assert_eq!(
        track.value_hash, replay_track.1.value_hash,
        "replayed track value hash mismatch"
    );

    let object_info = ctx
        .store
        .get_object_info(replay_track.0)
        .expect("read replayed object info");
    assert!(
        object_info.is_some(),
        "late node should have replayed uploaded object info"
    );

    let _ = harness.stop_all().await;
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
