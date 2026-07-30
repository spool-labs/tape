// Failure-injection matrix for one-call write resume. Each case leaves a tape
// in a distinct interrupted state via the staged SDK API (reserve / write_blob /
// upload / certify), then calls the one-call `write_named` and asserts it
// converges to a single certified, readable track without re-spending capacity
// or duplicating tracks. Resume detection reads track state from committee
// peers, so every case waits for the pre-state to be peer-visible first, which
// is the cross-process timing real resume runs under.
//
// Cases I and J cover the gateway's overwrite path instead: `write_or_resume_
// track_as` with a known prior track, which overwrites and reclaims rather than
// rejecting, plus `reclaim_object_as` for a multi-track stream. They assert the
// prior object's tracks are deleted whole (chunks and manifest), which on a
// healthy fleet returns the live track count to its baseline.

use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BasisPoints, ContentType, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_e2e_simnet::{run_simnet_test, NodeRuntimeMode, SimnetBuilder};
use tape_protocol::Api;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::stream::manifest::MAX_TRACK_SIZE;
use tape_sdk::tapedrive::Tapedrive;

const TARGET_GROUPS: u64 = 5;
const OBJECT_NAME: &str = "resume/obj";
// Simnet epochs auto-advance on wall-clock, so a multi-case run crosses several
// boundaries; reserve well past the whole run so no tape expires mid-test.
const EPOCHS: u64 = 40;

#[test]
fn resume_flow() {
    run_simnet_test(resume_flow_inner);
}

async fn resume_flow_inner() {
    let node_count = 20;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
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
        assert_eq!(
            scenario.self_advance_epoch(epoch_timeout).await.expect("epoch 2"),
            2
        );
        assert_eq!(
            scenario.self_advance_epoch(epoch_timeout).await.expect("epoch 3"),
            3
        );
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active at epoch 3");
    }

    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());

    let mut rng = StdRng::seed_from_u64(0x2E5_0000);
    let data = random_bytes(&mut rng, 128 * 1024);
    let other = random_bytes(&mut rng, 128 * 1024);
    let small = random_bytes(&mut rng, 512);
    let cap = StorageUnits::from_bytes(data.len() as u64) + StorageUnits::mb(1);

    // Case A: interrupted after reserve, before any register. Resume finds no
    // track and writes the blob fresh.
    {
        let key = TapeKey::generate();
        sdk.reserve(&key, cap, EPOCHS).await.expect("A reserve");
        let track = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("A resume completes the write");
        assert!(track.is_coded() && track.is_certified(), "A: coded+certified");
        assert_eq!(read_track(&sdk, &track, active_timeout).await, data, "A: data roundtrips");
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
    }

    // Case B: interrupted after register, before any slice upload. Resume adopts
    // the registered track, uploads its slices, and certifies it.
    {
        let key = TapeKey::generate();
        sdk.reserve(&key, cap, EPOCHS).await.expect("B reserve");
        sdk.write_named_blob(&key, OBJECT_NAME, ContentType::Unknown, &data)
            .await
            .expect("B register");
        wait_track_state(&sdk, &key.address(), TrackWant::Registered, active_timeout).await;
        let track = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("B resume finishes registered track");
        assert!(track.is_certified(), "B: certified after resume");
        assert_eq!(read_track(&sdk, &track, active_timeout).await, data, "B: data roundtrips");
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
    }

    // Case C: interrupted after register and slice upload, before certify.
    // Resume re-uploads (idempotent) and certifies.
    {
        let key = TapeKey::generate();
        sdk.reserve(&key, cap, EPOCHS).await.expect("C reserve");
        let (written, plan) = sdk
            .write_named_blob(&key, OBJECT_NAME, ContentType::Unknown, &data)
            .await
            .expect("C register");
        sdk.upload(&written, &plan).await.expect("C upload");
        wait_track_state(&sdk, &key.address(), TrackWant::Registered, active_timeout).await;
        let track = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("C resume certifies uploaded track");
        assert!(track.is_certified(), "C: certified after resume");
        assert_eq!(read_track(&sdk, &track, active_timeout).await, data, "C: data roundtrips");
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
    }

    // Case D: nothing was interrupted. A repeat of a completed write is a no-op
    // skip that spends no new capacity and adds no track.
    {
        let key = TapeKey::generate();
        let first = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("D first write");
        wait_track_state(&sdk, &key.address(), TrackWant::Certified, active_timeout).await;
        let used_before = sdk.get_tape(&key.address()).await.expect("D tape").used;

        let again = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("D resume skips");
        assert_eq!(again.track_number, first.track_number, "D: same track");
        assert!(again.is_certified(), "D: still certified");
        let used_after = sdk.get_tape(&key.address()).await.expect("D tape after").used;
        assert_eq!(used_after, used_before, "D: no new capacity spent");
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
    }

    // Case E: a tape key reused for different content is a conflict, never a
    // silent overwrite of the original track.
    {
        let key = TapeKey::generate();
        sdk.write_named(&key, OBJECT_NAME, ContentType::Unknown, &data, EPOCHS)
            .await
            .expect("E first write");
        wait_track_state(&sdk, &key.address(), TrackWant::Certified, active_timeout).await;
        let err = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &other, EPOCHS)
            .await
            .expect_err("E: different content must conflict");
        assert!(
            matches!(err, TapedriveError::WriteConflict { .. }),
            "E: expected WriteConflict, got {err:?}"
        );
        assert_eq!(read_track_by_number(&sdk, &key.address(), active_timeout).await, data, "E: original intact");
    }

    // Case F: an inline write certifies at register, so a repeat is an
    // idempotent skip with nothing to finish.
    {
        let key = TapeKey::generate();
        let small_cap = StorageUnits::from_bytes(small.len() as u64) + StorageUnits::mb(1);
        sdk.reserve(&key, small_cap, EPOCHS).await.expect("F reserve");
        let inline = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &small, EPOCHS)
            .await
            .expect("F inline write");
        assert!(inline.is_inline() && inline.is_certified(), "F: inline+certified");
        wait_track_state(&sdk, &key.address(), TrackWant::Certified, active_timeout).await;
        let again = sdk
            .write_named(&key, OBJECT_NAME, ContentType::Unknown, &small, EPOCHS)
            .await
            .expect("F inline resume skips");
        assert_eq!(again.track_number, inline.track_number, "F: same track");
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
    }

    // Case G: interrupted multi-track stream. Register only chunk 0 (uncertified),
    // then the one-call stream write finishes chunk 0, writes the remaining chunk,
    // and writes the manifest. A 3-track tape (two chunks + manifest) reads back.
    {
        let key = TapeKey::generate();
        let stream_data = random_bytes(&mut rng, MAX_TRACK_SIZE + 1024);
        let cap = StorageUnits::from_bytes(stream_data.len() as u64) + StorageUnits::mb(2);
        sdk.reserve(&key, cap, EPOCHS).await.expect("G reserve");

        let first = &stream_data[0..MAX_TRACK_SIZE];
        sdk.write_blob(&key, first).await.expect("G register chunk 0");
        wait_track_state(&sdk, &key.address(), TrackWant::Registered, active_timeout).await;

        let receipt = sdk
            .write_named_bytes(&key, OBJECT_NAME, ContentType::Unknown, &stream_data)
            .await
            .expect("G resume stream");
        let got = read_bytes_retry(&sdk, &receipt.manifest, active_timeout).await;
        assert_eq!(got, stream_data, "G: stream roundtrips");
        wait_track_count(&sdk, &key.address(), 3, active_timeout).await;
    }

    // Case H: interrupted reader stream. Register chunk 0, then the one-call
    // reader stream write peeks chunk 0, detects the resume, finishes it, and
    // reads the remaining chunk from the reader before writing the manifest.
    {
        let key = TapeKey::generate();
        let stream_data = random_bytes(&mut rng, MAX_TRACK_SIZE + 2048);
        let cap = StorageUnits::from_bytes(stream_data.len() as u64) + StorageUnits::mb(2);
        sdk.reserve(&key, cap, EPOCHS).await.expect("H reserve");

        let first = &stream_data[0..MAX_TRACK_SIZE];
        sdk.write_blob(&key, first).await.expect("H register chunk 0");
        wait_track_state(&sdk, &key.address(), TrackWant::Registered, active_timeout).await;

        let size = StorageUnits::from_bytes(stream_data.len() as u64);
        let receipt = sdk
            .write_named_stream(&key, OBJECT_NAME, ContentType::Unknown, size, stream_data.as_slice())
            .await
            .expect("H resume reader stream");
        let got = read_bytes_retry(&sdk, &receipt.manifest, active_timeout).await;
        assert_eq!(got, stream_data, "H: reader stream roundtrips");
        wait_track_count(&sdk, &key.address(), 3, active_timeout).await;
    }

    // Case I: gateway single-track overwrite. A certified track re-put under the
    // same name with different content overwrites: a new track is written and the
    // prior one reclaimed, so the tape ends with exactly one live track.
    {
        let key = TapeKey::generate();
        sdk.reserve(&key, cap, EPOCHS).await.expect("I reserve");
        let first = sdk
            .write_or_resume_track_as(&key, OBJECT_NAME, ContentType::Unknown, &data, None)
            .await
            .expect("I first write");
        wait_track_state(&sdk, &key.address(), TrackWant::Certified, active_timeout).await;
        let second = sdk
            .write_or_resume_track_as(
                &key,
                OBJECT_NAME,
                ContentType::Unknown,
                &other,
                Some(track_pda(key.address(), first.track.track_number).0),
            )
            .await
            .expect("I overwrite");
        assert_ne!(
            second.track.track_number, first.track.track_number,
            "I: overwrite writes a new track"
        );
        wait_track_count(&sdk, &key.address(), 1, active_timeout).await;
        assert_eq!(read_track(&sdk, &second.track, active_timeout).await, other, "I: reads overwritten bytes");
    }

    // Case J: gateway stream overwrite. A multi-track stream re-put under the same
    // name writes a fresh stream, then reclaims the prior manifest whole: every
    // old chunk and its manifest are deleted, returning the live count to the new
    // stream's three tracks with the new stream still readable.
    {
        let key = TapeKey::generate();
        let stream_v1 = random_bytes(&mut rng, MAX_TRACK_SIZE + 4096);
        let stream_v2 = random_bytes(&mut rng, MAX_TRACK_SIZE + 8192);
        let stream_cap = StorageUnits::from_bytes((stream_v1.len() + stream_v2.len()) as u64)
            + StorageUnits::mb(2);
        sdk.reserve(&key, stream_cap, EPOCHS).await.expect("J reserve");

        let first = sdk
            .write_named_bytes_as(&key, OBJECT_NAME, ContentType::Unknown, &stream_v1)
            .await
            .expect("J stream v1");
        wait_track_count(&sdk, &key.address(), 3, active_timeout).await;

        let second = sdk
            .write_named_bytes_as(&key, OBJECT_NAME, ContentType::Unknown, &stream_v2)
            .await
            .expect("J stream v2");
        wait_track_count(&sdk, &key.address(), 6, active_timeout).await;

        sdk.reclaim_object_as(&key, first.manifest).await.expect("J reclaim old stream");
        wait_track_count(&sdk, &key.address(), 3, active_timeout).await;

        let got = read_bytes_retry(&sdk, &second.manifest, active_timeout).await;
        assert_eq!(got, stream_v2, "J: new stream intact after reclaim");
    }

    harness.stop_all().await.expect("stop runtimes");
}

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rng.fill_bytes(&mut bytes);
    bytes
}

#[derive(Clone, Copy, Debug)]
enum TrackWant {
    Registered,
    Certified,
}

/// Wait until track 0 of `tape` is peer-visible in the wanted state. Resume
/// reads state from peers, so a case must not invoke resume until its pre-state
/// has propagated, matching the cross-process delay real resume runs after.
async fn wait_track_state<B: Rpc, C: Api>(
    sdk: &Tapedrive<B, C>,
    tape: &Address,
    want: TrackWant,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        if let Ok(track) = sdk.get_track_by_number(tape, TrackNumber(0)).await {
            let matched = match want {
                TrackWant::Registered => track.is_registered(),
                TrackWant::Certified => track.is_certified(),
            };
            if matched {
                return;
            }
        }
        if start.elapsed() >= timeout {
            panic!("timed out waiting for track 0 on {tape} to reach {want:?}");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Wait until the tape lists exactly `expected` tracks. The listing query races
/// peers and a not-yet-synced peer can answer with a short list, so poll rather
/// than assert once; a genuinely wrong count (e.g. a duplicate) never reaches
/// `expected` and the timeout reports what was actually seen.
async fn wait_track_count<B: Rpc, C: Api>(
    sdk: &Tapedrive<B, C>,
    tape: &Address,
    expected: usize,
    timeout: Duration,
) {
    let start = Instant::now();
    let mut last = None;
    loop {
        if let Ok((tracks, _)) = sdk.list_tracks_by_tape(tape, None, 16).await {
            if tracks.len() == expected {
                return;
            }
            last = Some(tracks.len());
        }
        if start.elapsed() >= timeout {
            panic!("track count for {tape} = {last:?}, expected {expected}");
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// Read a coded track, retrying until its slices are servable. Certify returns
/// once quorum has signed, but a fresh read can briefly race slice indexing and
/// epoch-boundary spool migration, so poll rather than read once.
async fn read_track<B: Rpc, C: Api>(
    sdk: &Tapedrive<B, C>,
    track: &CompressedTrack,
    timeout: Duration,
) -> Vec<u8> {
    let address = track_pda(track.tape, track.track_number).0;
    let start = Instant::now();
    loop {
        match sdk.read(&address).await {
            Ok(data) => return data,
            Err(error) if start.elapsed() >= timeout => {
                panic!("read {address} timed out: {error:?}");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(400)).await,
        }
    }
}

/// Read a stream by its manifest address, retrying while slices propagate.
async fn read_bytes_retry<B: Rpc, C: Api>(
    sdk: &Tapedrive<B, C>,
    manifest: &Address,
    timeout: Duration,
) -> Vec<u8> {
    let start = Instant::now();
    loop {
        match sdk.read_bytes(manifest).await {
            Ok(data) => return data,
            Err(error) if start.elapsed() >= timeout => {
                panic!("read_bytes {manifest} timed out: {error:?}");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(400)).await,
        }
    }
}

async fn read_track_by_number<B: Rpc, C: Api>(
    sdk: &Tapedrive<B, C>,
    tape: &Address,
    timeout: Duration,
) -> Vec<u8> {
    let track = sdk
        .get_track_by_number(tape, TrackNumber(0))
        .await
        .expect("get track 0");
    read_track(sdk, &track, timeout).await
}
