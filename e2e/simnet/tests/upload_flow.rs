use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_api::program::tapedrive::track_pda;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetScenario, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::stream::manifest::MAX_TRACK_SIZE;

const TARGET_GROUPS: u64 = 5;

#[test]
fn upload_flow() {
    run_simnet_test(upload_flow_inner);
}

async fn upload_flow_inner() {
    let node_count = 20;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    let health_timeout = Duration::from_secs(30);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
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

    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    {
        let scenario = harness.scenario();

        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .expect("nodes healthy");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active");

        let epoch2 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 2");
        assert_eq!(epoch2, 2, "expected epoch 2");

        let epoch3 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 3");
        assert_eq!(epoch3, 3, "expected epoch 3");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active at epoch 3");
        assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
    }

    let scenario = harness.scenario();

    let mut rng = StdRng::seed_from_u64(0xDA7A_51CE);
    let raw_data = random_bytes(&mut rng, 512);
    let blob_data = random_bytes(&mut rng, 128 * 1024);
    let stream_data = random_bytes(&mut rng, MAX_TRACK_SIZE + 1024);
    let reserve_capacity = StorageUnits::from_bytes(
        (raw_data.len() + blob_data.len() + stream_data.len()) as u64,
    ) + StorageUnits::mb(2);

    let sdk = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let tape_address = tape_key.address();
    sdk.reserve(&tape_key, reserve_capacity, 4)
        .await
        .expect("reserve tape");

    let raw_track = sdk
        .write_raw(&tape_key, &raw_data)
        .await
        .expect("write raw track");
    assert!(raw_track.is_inline(), "raw write should create a raw track");
    assert!(raw_track.is_certified(), "raw track should be certified");
    assert_eq!(raw_track.tape, tape_address, "raw track tape mismatch");
    assert!(
        raw_track.group.0 < TARGET_GROUPS,
        "raw track assigned outside live groups"
    );

    let blob_track = sdk
        .write_track(&tape_key, &blob_data)
        .await
        .expect("write blob track");
    assert!(blob_track.is_coded(), "blob write should create a blob track");
    assert!(
        blob_track.is_certified(),
        "blob track should be certified"
    );
    assert_eq!(blob_track.tape, tape_address, "blob track tape mismatch");
    assert!(
        blob_track.group.0 < TARGET_GROUPS,
        "blob track assigned outside live groups"
    );

    let receipt = sdk
        .write_bytes(&tape_key, &stream_data)
        .await
        .expect("write stream");
    assert_eq!(receipt.tape, tape_address, "stream tape mismatch");
    assert!(
        receipt.manifest_track_number.0 >= 4,
        "stream should span multiple chunk tracks before the manifest"
    );

    let raw_address = track_pda(raw_track.tape, raw_track.track_number).0;
    let blob_address = track_pda(blob_track.tape, blob_track.track_number).0;

    let start = Instant::now();
    let (tracks, next_cursor) = loop {
        match sdk.list_tracks_by_tape(&tape_address, None, 10).await {
            Ok((tracks, next_cursor)) if tracks.len() == 5 && next_cursor.is_none() => {
                break (tracks, next_cursor);
            }
            Ok((tracks, next_cursor)) if start.elapsed() >= active_timeout => {
                panic!(
                    "timed out waiting for tape track list, observed {} tracks and cursor {:?}",
                    tracks.len(),
                    next_cursor
                );
            }
            Err(error) if start.elapsed() >= active_timeout => {
                panic!("timed out waiting for tape track list: {error}");
            }
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    };
    wait_coded_track_slices(&scenario, &tracks, active_timeout)
        .await
        .expect("coded track slices stored by current owners");

    let raw_read = sdk.read(&raw_address).await.expect("read raw track");
    assert_eq!(raw_read, raw_data, "raw read should match original data");

    let blob_read = sdk.read(&blob_address).await.expect("read blob track");
    assert_eq!(blob_read, blob_data, "blob read should match original data");

    let stream_read = sdk
        .read_bytes(&receipt.manifest)
        .await
        .expect("read stream");
    assert_eq!(
        stream_read, stream_data,
        "stream read should match original data"
    );

    assert_eq!(next_cursor, None, "unexpected track pagination cursor");
    assert_eq!(
        tracks.len(),
        5,
        "same tape should contain raw, blob, two stream chunks, and manifest"
    );
    assert_eq!(
        tracks.iter().filter(|track| track.is_inline()).count(),
        2,
        "expected raw track plus inline stream manifest on tape"
    );
    assert_eq!(
        tracks.iter().filter(|track| track.is_coded()).count(),
        3,
        "expected single blob plus two stream chunk tracks"
    );

    for track in tracks.iter().filter(|track| track.is_coded()) {
        assert!(track.is_certified(), "blob track should be certified");
        assert!(
            track.group.0 < TARGET_GROUPS,
            "blob track assigned outside live groups"
        );

        let track_address = track_pda(track.tape, track.track_number).0;
        let slice_count = scenario
            .count_slices(&track_address, track.group)
            .expect("count blob slices");
        assert_eq!(
            slice_count, GROUP_SIZE,
            "blob track should be stored across the full group"
        );
    }

    harness.stop_all().await.expect("stop runtimes");
}

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rng.fill_bytes(&mut bytes);
    bytes
}

async fn wait_coded_track_slices(
    scenario: &SimnetScenario<'_>,
    tracks: &[CompressedTrack],
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();

    loop {
        let mut pending = Vec::new();
        for track in tracks.iter().filter(|track| track.is_coded()) {
            let track_address = track_pda(track.tape, track.track_number).0;
            let observed = scenario
                .count_current_owner_slices(&track_address, track.group)
                .await?;
            if observed != GROUP_SIZE {
                pending.push(format!(
                    "track {} group {}: {observed}/{GROUP_SIZE}",
                    track.track_number.0, track.group.0
                ));
            }
        }

        if pending.is_empty() {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            anyhow::bail!(
                "timed out waiting for current owners to store coded slices: {}",
                pending.join(", ")
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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
