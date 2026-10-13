use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::types::{BasisPoints, ContentType, StorageUnits};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::metrics::{InMemory, Operation, Phase};
use tape_sdk::object::{ListObjectsQuery, ObjectBatchItem};
use tape_sdk::progress::{Progress, Step, UnitKind};

const TARGET_GROUPS: u64 = 1;

/// Keeps every step the client reports
#[derive(Default)]
struct Recorder {
    steps: Mutex<Vec<Step>>,
}

impl Recorder {
    /// Hand back the steps so far and start over
    fn take(&self) -> Vec<Step> {
        std::mem::take(&mut *self.steps.lock().expect("steps lock"))
    }
}

impl Progress for Recorder {
    fn report(&self, step: Step) {
        self.steps.lock().expect("steps lock").push(step);
    }
}

/// The units whose registers went out, in order, and how many tracks are done
fn registered_and_done(steps: &[Step]) -> (Vec<usize>, usize) {
    let mut registered = Vec::new();
    let mut done = 0;
    for step in steps {
        if let Step::Registering { unit } = step {
            registered.push(*unit);
        }
        if let Step::Done { .. } = step {
            done += 1;
        }
    }
    (registered, done)
}

#[test]
fn object_layer_round_trip() {
    run_simnet_test(object_layer_round_trip_inner);
}

async fn object_layer_round_trip_inner() {
    let node_count = TAPENET_GROUP_SIZE;
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

    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("all nodes active");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");
    assert_eq!(epoch, 2, "expected epoch 2");

    let metrics = Arc::new(InMemory::new());
    let progress = Arc::new(Recorder::default());
    let sdk = scenario
        .sdk(harness.admin())
        .with_metrics(metrics.clone())
        .with_progress(progress.clone());
    let bucket = TapeKey::generate();
    let name = "photos/cat.jpg";
    let data = b"named object bytes";

    sdk.reserve(&bucket, StorageUnits::mb(1), 4)
        .await
        .expect("reserve bucket tape");

    sdk.put_object(&bucket, name, data, Some("image/jpeg"))
        .await
        .expect("put object");

    let listed = wait_for_objects(&sdk, &bucket.address(), "photos/", &[name], Duration::from_secs(10))
        .await;
    assert_eq!(listed, vec![name.to_string()], "object list mismatch");

    let read = sdk
        .get_object(&bucket.address(), name)
        .await
        .expect("get object");
    assert_eq!(read, data, "object bytes should round-trip");

    sdk.delete_object(&bucket, name).await.expect("delete object");

    let listed = wait_for_objects(&sdk, &bucket.address(), "photos/", &[], Duration::from_secs(10))
        .await;
    assert!(listed.is_empty(), "object list should be empty after delete");

    // A mixed inline/coded batch retains caller order, certifies every coded
    // track, and leaves all objects independently readable. The entry point
    // is deliberately last, matching the site publication contract.
    let script = b"console.log('batch');".to_vec();
    let stylesheet = vec![b'x'; 4 * 1024];
    let index = b"<html><script src=\"app.js\"></script></html>".to_vec();
    let batch = vec![
        ObjectBatchItem {
            name: "app.js".into(),
            data: script.clone(),
            content_type: ContentType::TextJavascript,
            plan: None,
        },
        ObjectBatchItem {
            name: "style.css".into(),
            data: stylesheet.clone(),
            content_type: ContentType::TextCss,
            plan: None,
        },
        ObjectBatchItem {
            name: "index.html".into(),
            data: index.clone(),
            content_type: ContentType::TextHtml,
            plan: None,
        },
    ];
    progress.take();
    let stored = sdk
        .store_objects_batch(&bucket, batch)
        .await
        .expect("store ordered object batch");
    let steps = progress.take();
    assert!(
        matches!(steps.first(), Some(Step::Planned { kind: UnitKind::Track, sizes }) if sizes.len() == 3),
        "a batch should plan every object as a track before anything else"
    );
    let (registered, done) = registered_and_done(&steps);
    assert_eq!(registered, [0, 1, 2], "registers should be reported in caller order");
    assert_eq!(done, 2, "an inline object should be done once its register lands");
    let receipts = stored.receipts;
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.name.as_str())
            .collect::<Vec<_>>(),
        vec!["app.js", "style.css", "index.html"],
        "batch receipts must retain caller order"
    );
    let stylesheet_track = sdk
        .get_track(&receipts[1].address)
        .await
        .expect("fetch stored coded stylesheet");
    assert!(
        !stylesheet_track.is_certified(),
        "coded batch object should remain pending at the optimistic boundary"
    );
    metrics.clear();
    sdk.certify_objects_batch(&bucket, stored.verification)
        .await
        .expect("certify stored object batch");
    let (_, done) = registered_and_done(&progress.take());
    assert_eq!(done, 1, "the coded object should be done once its certify lands");
    // The store half's mirror carries into certification, so the one coded
    // object certifies without ever asking a peer for a proof.
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifySubmit),
        1,
        "the coded batch object should certify exactly once"
    );
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifyProof),
        0,
        "batch certification should prove tracks against the carried mirror"
    );
    let stylesheet_track = wait_for_certified_track(
        &sdk,
        &receipts[1].address,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        stylesheet_track.is_certified(),
        "coded batch object should certify through its verification handle"
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "app.js")
            .await
            .expect("read batch script"),
        script
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "style.css")
            .await
            .expect("read coded batch stylesheet"),
        stylesheet
    );
    assert_eq!(
        sdk.get_object(&bucket.address(), "index.html")
            .await
            .expect("read batch entry point"),
        index
    );

    // Certifying one retained batch must not stale a later handle.
    let earlier = sdk
        .store_objects_batch(
            &bucket,
            vec![ObjectBatchItem {
                name: "earlier.css".into(),
                data: vec![b'a'; 4 * 1024],
                content_type: ContentType::TextCss,
                plan: None,
            }],
        )
        .await
        .expect("store earlier deferred batch");
    let later = sdk
        .store_objects_batch(
            &bucket,
            vec![ObjectBatchItem {
                name: "later.css".into(),
                data: vec![b'b'; 4 * 1024],
                content_type: ContentType::TextCss,
                plan: None,
            }],
        )
        .await
        .expect("store later deferred batch");
    let later_track = later.receipts[0].address;

    sdk.certify_objects_batch(&bucket, earlier.verification)
        .await
        .expect("certify earlier deferred batch");
    metrics.clear();
    sdk.certify_objects_batch(&bucket, later.verification)
        .await
        .expect("certify batch after root-only conflict");
    assert!(
        batch_phase_count(&metrics, Phase::CertifyProof) > 0,
        "root-only conflict should fall back to a fresh peer proof"
    );
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifySubmit),
        batch_phase_count(&metrics, Phase::CertifyProof),
        "a submit without a proof fetch is the doomed submit on the stale mirror"
    );
    let later_track = wait_for_certified_track(&sdk, &later_track, Duration::from_secs(10)).await;
    assert!(
        later_track.is_certified(),
        "batch should certify after another handle changes the tape root"
    );

    // Repository tracks certified after a deferred site batch must not stale it.
    let mut repo_tracks = Vec::new();
    for byte in [b'p', b'q'] {
        let pack = vec![byte; 4 * 1024];
        let stored = sdk
            .store_named_object(
                &bucket,
                b"",
                ContentType::Unknown,
                StorageUnits::from_bytes(pack.len() as u64),
                pack.as_slice(),
            )
            .await
            .expect("store repository pack");
        repo_tracks.push(stored);
    }
    let refs = vec![b'r'; 4 * 1024];
    let (refs_written, refs_plan) = sdk
        .write_named_blob(&bucket, "refs.idx", ContentType::Unknown, &refs)
        .await
        .expect("write repository index");
    let refs_receipts = sdk
        .upload(&refs_written, &refs_plan)
        .await
        .expect("upload repository index");
    repo_tracks.push((refs_written, refs_receipts));

    let site = sdk
        .store_objects_batch(
            &bucket,
            vec![
                ObjectBatchItem {
                    name: "site/main.css".into(),
                    data: vec![b'c'; 4 * 1024],
                    content_type: ContentType::TextCss,
                    plan: None,
                },
                ObjectBatchItem {
                    name: "site/main.js".into(),
                    data: vec![b'j'; 4 * 1024],
                    content_type: ContentType::TextJavascript,
                    plan: None,
                },
            ],
        )
        .await
        .expect("store site batch");
    let mut site_tracks = Vec::new();
    for receipt in &site.receipts {
        site_tracks.push(receipt.address);
    }

    for (written, receipts) in &repo_tracks {
        sdk.certify_with_receipts(&bucket, written, receipts)
            .await
            .expect("certify repository track");
    }

    metrics.clear();
    sdk.certify_objects_batch(&bucket, site.verification)
        .await
        .expect("certify site batch behind repository tracks");
    assert!(
        batch_phase_count(&metrics, Phase::CertifyProof) > 0,
        "repository certifications should force fresh peer proofs"
    );
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifySubmit),
        batch_phase_count(&metrics, Phase::CertifyProof),
        "a submit without a proof fetch is the doomed submit on the stale mirror"
    );
    for address in &site_tracks {
        let site_track = wait_for_certified_track(&sdk, address, Duration::from_secs(10)).await;
        assert!(
            site_track.is_certified(),
            "site object should certify behind the repository tracks"
        );
    }

    metrics.clear();
    let fully_put = sdk
        .put_objects_batch(
            &bucket,
            vec![ObjectBatchItem {
                name: "complete.css".into(),
                data: vec![b'y'; 4 * 1024],
                content_type: ContentType::TextCss,
                plan: None,
            }],
        )
        .await
        .expect("put fully verified object batch");
    let complete_track = wait_for_certified_track(
        &sdk,
        &fully_put[0].address,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        complete_track.is_certified(),
        "put_objects_batch should retain its fully verified contract"
    );
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifySubmit),
        1,
        "the fully verified batch should certify exactly once"
    );
    assert_eq!(
        batch_phase_count(&metrics, Phase::CertifyProof),
        0,
        "put_objects_batch should prove its track against its own mirror"
    );

    // Two batches writing one bucket at once interleave their registers, which
    // costs each handle its mirror and must not fail either write.
    let left_first = vec![b'm'; 4 * 1024];
    let left_second = vec![b'n'; 4 * 1024];
    let right_first = vec![b'o'; 4 * 1024];
    let right_second = vec![b'q'; 4 * 1024];
    let (left, right) = tokio::join!(
        sdk.store_objects_batch(
            &bucket,
            vec![
                ObjectBatchItem {
                    name: "left/0.css".into(),
                    data: left_first.clone(),
                    content_type: ContentType::TextCss,
                    plan: None,
                },
                ObjectBatchItem {
                    name: "left/1.css".into(),
                    data: left_second.clone(),
                    content_type: ContentType::TextCss,
                    plan: None,
                },
            ],
        ),
        sdk.store_objects_batch(
            &bucket,
            vec![
                ObjectBatchItem {
                    name: "right/0.css".into(),
                    data: right_first.clone(),
                    content_type: ContentType::TextCss,
                    plan: None,
                },
                ObjectBatchItem {
                    name: "right/1.css".into(),
                    data: right_second.clone(),
                    content_type: ContentType::TextCss,
                    plan: None,
                },
            ],
        ),
    );
    let left = left.expect("store left concurrent batch");
    let right = right.expect("store right concurrent batch");

    let (left_certified, right_certified) = tokio::join!(
        sdk.certify_objects_batch(&bucket, left.verification),
        sdk.certify_objects_batch(&bucket, right.verification),
    );
    left_certified.expect("certify left concurrent batch");
    right_certified.expect("certify right concurrent batch");

    let mut concurrent_receipts = left.receipts;
    concurrent_receipts.extend(right.receipts);
    for receipt in &concurrent_receipts {
        let track =
            wait_for_certified_track(&sdk, &receipt.address, Duration::from_secs(10)).await;
        assert!(
            track.is_certified(),
            "object written under a concurrent batch should certify"
        );
    }

    let concurrent_objects = [
        ("left/0.css", &left_first),
        ("left/1.css", &left_second),
        ("right/0.css", &right_first),
        ("right/1.css", &right_second),
    ];
    for (name, data) in concurrent_objects {
        let read = sdk
            .get_object(&bucket.address(), name)
            .await
            .expect("read concurrent batch object");
        assert_eq!(&read, data, "concurrent batch object should round-trip");
    }

    harness.stop_all().await.expect("stop runtimes");
}

fn batch_phase_count(metrics: &InMemory, phase: Phase) -> u64 {
    let mut count = 0;
    for (key, summary) in metrics.snapshot() {
        if key.operation == Operation::WriteBatch && key.phase == phase {
            count += summary.count;
        }
    }
    count
}

async fn wait_for_certified_track(
    sdk: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, tape_peer_http::HttpApi>,
    track: &tape_crypto::Address,
    timeout: Duration,
) -> tape_core::prelude::CompressedTrack {
    let start = Instant::now();
    loop {
        let current = sdk.get_track(track).await.expect("fetch batch track");
        if current.is_certified() || start.elapsed() >= timeout {
            return current;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_objects(
    sdk: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, tape_peer_http::HttpApi>,
    bucket: &tape_crypto::Address,
    prefix: &str,
    expected: &[&str],
    timeout: Duration,
) -> Vec<String> {
    let start = Instant::now();
    loop {
        let objects = sdk
            .list_objects(bucket, ListObjectsQuery::new(prefix))
            .await
            .expect("list objects");
        let objects = objects
            .objects
            .into_iter()
            .map(|object| {
                String::from_utf8(object.name).expect("object name should be utf-8")
            })
            .collect::<Vec<_>>();
        let expected: Vec<String> = expected.iter().map(|name| (*name).to_string()).collect();
        if objects == expected || start.elapsed() >= timeout {
            return objects;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
