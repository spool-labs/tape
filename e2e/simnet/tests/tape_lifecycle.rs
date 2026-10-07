// A tape is rewound twice in two epochs, an expired tape is renewed, and a
// tape is closed and reserved again in one epoch. The epochs keep turning
// through all of it, which takes every node building the same assignment
// candidate, and a node restarted in the middle of an epoch stays in step.

use std::time::{Duration, Instant};

use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{BasisPoints, ContentType, EpochNumber, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, run_simnet_test};
use tape_peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use tape_sdk::balance::sol_balance_of;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::object::ListObjectsQuery;
use tape_sdk::tapedrive::Tapedrive;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
// Simnet epochs advance on wall-clock, so the long tapes are reserved well past the run.
const EPOCHS: u64 = 40;
// The short tape expires two epochs after it is reserved, and is renewed then.
const SHORT_EPOCHS: u64 = 2;
const RENEWED_EPOCHS: u64 = 10;
const CAPACITY_MB: u64 = 8;
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);
const RESTARTED_NODE: usize = 0;

type Client = Tapedrive<LiteSvmRpc, HttpApi>;

#[test]
fn tape_lifecycle() {
    run_simnet_test(tape_lifecycle_inner);
}

async fn tape_lifecycle_inner() {
    let node_count = TAPENET_GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();

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
    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(Duration::from_secs(30))
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
    }

    let owner = harness.scenario().sdk(harness.admin());
    let capacity = StorageUnits::mb(CAPACITY_MB);
    let page = vec![0x5Au8; 64 * 1024];
    let rewritten = vec![0xA5u8; 64 * 1024];
    let note = b"one ledger line".to_vec();

    // Three tapes: one to rewind, one that expires and is renewed, one to
    // close and reserve again.
    let rewound = TapeKey::generate();
    let short = TapeKey::generate();
    let closed = TapeKey::generate();
    let rewound_tape = rewound.address();
    let short_tape = short.address();
    let closed_tape = closed.address();

    owner.reserve(&rewound, capacity, EPOCHS).await.expect("reserve the tape to rewind");
    let short_reserved = owner
        .reserve(&short, capacity, SHORT_EPOCHS)
        .await
        .expect("reserve the short tape");
    let closed_reserved = owner
        .reserve(&closed, capacity, EPOCHS)
        .await
        .expect("reserve the tape to close");

    owner
        .put_object(&rewound, "docs/page.bin", &page, None)
        .await
        .expect("coded write");
    owner
        .put_object(&rewound, "docs/note.txt", &note, Some("text/plain"))
        .await
        .expect("inline write");

    // A third track through the one-call write. Asked to resume, the same
    // call finds that track and writes nothing.
    let resuming = harness.scenario().sdk(harness.admin()).with_write_options(WriteOptions {
        resume: true,
        ..WriteOptions::default()
    });
    let before_rewind = resuming
        .write_named(&rewound, "docs/log.bin", ContentType::Unknown, &page, EPOCHS)
        .await
        .expect("one-call write");
    let repeated = resuming
        .write_named(&rewound, "docs/log.bin", ContentType::Unknown, &page, EPOCHS)
        .await
        .expect("resumed write");
    assert_eq!(repeated.address(), before_rewind.address());

    owner
        .put_object(&short, "old/page.bin", &page, None)
        .await
        .expect("write to the short tape");
    owner
        .put_object(&closed, "data/file.bin", &page, None)
        .await
        .expect("write to the tape to close");

    let first_tracks = wait_tracks(&owner, &rewound_tape, 3).await;
    wait_tracks(&owner, &short_tape, 1).await;
    let closed_tracks = wait_tracks(&owner, &closed_tape, 1).await;
    assert_eq!(closed_tracks[0].track_number, TrackNumber(0));

    // The tracks are counted from the next epoch on, so each removal below
    // lands on tracks the round weighs.
    advance(&harness, epoch_timeout, 3).await;

    // First rewind. The old tracks leave the listing at once, and a write
    // takes the next track number.
    let account = owner.rewind_tape(&rewound).await.expect("rewind the tape");
    wait_tracks(&owner, &rewound_tape, 0).await;

    assert_eq!(account.tracks.num_tracks(), 0);
    assert_eq!(account.tracks.tree_start(), TrackNumber(3));

    // A resumed write of content the tape held before the rewind finds no
    // track to finish or reuse, and writes a new one.
    let resumed = resuming
        .write_named(&rewound, "docs/log.bin", ContentType::Unknown, &page, EPOCHS)
        .await
        .expect("resumed write after the rewind");
    assert_eq!(resumed.track_number, TrackNumber(3));
    assert_ne!(resumed.address(), before_rewind.address());

    owner
        .put_object(&rewound, "docs/page.bin", &rewritten, None)
        .await
        .expect("write after the rewind");
    let after_rewind = wait_tracks(&owner, &rewound_tape, 2).await;
    assert_eq!(after_rewind[1].track_number, TrackNumber(4));
    assert_eq!(read_object(&owner, &rewound_tape, "docs/page.bin").await, rewritten);
    assert!(owner.get_object(&rewound_tape, "docs/note.txt").await.is_err());

    // A node restarted in the middle of the epoch still counts the tracks
    // that left in it.
    harness
        .stop_nodes(&[RESTARTED_NODE])
        .await
        .expect("stop a node");
    harness
        .start_nodes_with_retry(&[RESTARTED_NODE], 3, Duration::from_millis(200))
        .await
        .expect("start the node again");

    let epoch = advance(&harness, epoch_timeout, 4).await;

    // Second rewind, an epoch after the first.
    owner.rewind_tape(&rewound).await.expect("rewind the tape again");
    wait_tracks(&owner, &rewound_tape, 0).await;
    owner
        .put_object(&rewound, "docs/page.bin", &page, None)
        .await
        .expect("write after the second rewind");
    let after_second = wait_tracks(&owner, &rewound_tape, 1).await;
    assert_eq!(after_second[0].track_number, TrackNumber(5));

    // The short tape has expired. A renew brings it back under its number.
    assert!(EpochNumber(epoch) >= short_reserved.expiry_epoch, "the short tape has expired");
    assert!(owner.put_object(&short, "late/page.bin", &page, None).await.is_err());

    let renewed = owner
        .renew_tape(&short, capacity, RENEWED_EPOCHS)
        .await
        .expect("renew the short tape");
    assert_eq!(renewed.id, short_reserved.id);
    assert_eq!(
        renewed.expiry_epoch,
        renewed.active_epoch.saturating_add(EpochNumber(RENEWED_EPOCHS))
    );
    assert_eq!(renewed.tracks.tree_start(), TrackNumber(1));
    wait_tracks(&owner, &short_tape, 0).await;

    owner
        .put_object(&short, "new/page.bin", &rewritten, None)
        .await
        .expect("write to the renewed tape");
    let on_renewed = wait_tracks(&owner, &short_tape, 1).await;
    assert_eq!(on_renewed[0].track_number, TrackNumber(1));
    assert_eq!(read_object(&owner, &short_tape, "new/page.bin").await, rewritten);

    advance(&harness, epoch_timeout, 5).await;

    // Close a tape and reserve its address again in the same epoch. The new
    // tape has a new number and starts at track 0, under the name the old
    // tape's track 0 had.
    let closed_key: Address = closed.pubkey().into();
    let key_before = sol_balance_of(owner.rpc(), &closed_key).await.expect("tape key balance");
    let wallet_before = owner.sol_balance().await.expect("wallet balance");

    owner.close_tape(&closed).await.expect("close the tape");
    assert!(owner.get_tape(&closed_tape).await.is_err(), "the tape account is closed");
    wait_tracks(&owner, &closed_tape, 0).await;

    // The tape's rent went past the tape key to the wallet that paid it.
    let key_after = sol_balance_of(owner.rpc(), &closed_key).await.expect("tape key balance");
    let wallet_after = owner.sol_balance().await.expect("wallet balance");
    assert_eq!(key_after, key_before);
    assert!(wallet_after > wallet_before, "the wallet got the rent back");

    let reserved_again = owner
        .reserve(&closed, capacity, EPOCHS)
        .await
        .expect("reserve the address again");
    assert_ne!(reserved_again.id, closed_reserved.id);

    owner
        .put_object(&closed, "data/file.bin", &rewritten, None)
        .await
        .expect("write to the new tape");
    let on_new = wait_tracks(&owner, &closed_tape, 1).await;
    assert_eq!(on_new[0].track_number, TrackNumber(0));
    assert_eq!(on_new[0].tape_number, reserved_again.id);
    assert_ne!(on_new[0].address(), closed_tracks[0].address());
    assert_eq!(read_object(&owner, &closed_tape, "data/file.bin").await, rewritten);

    advance(&harness, epoch_timeout, 6).await;

    // Once the epoch a track left in is over, the nodes delete it.
    for old in [first_tracks[0].address(), closed_tracks[0].address()] {
        wait_gone(&owner, old).await;
    }

    // Deleting the closed tape's track 0 left the new tape's object listed.
    let listed = owner
        .list_objects(&closed_tape, ListObjectsQuery::new("data/"))
        .await
        .expect("list objects");
    let names: Vec<Vec<u8>> = listed.objects.into_iter().map(|object| object.name).collect();
    assert_eq!(names, vec![b"data/file.bin".to_vec()]);
    assert_eq!(read_object(&owner, &closed_tape, "data/file.bin").await, rewritten);
    assert_eq!(read_object(&owner, &rewound_tape, "docs/page.bin").await, page);
}

/// Wait for the next epoch and return its number
async fn advance(
    harness: &tape_e2e_simnet::SimnetHarness,
    timeout: Duration,
    expected: u64,
) -> u64 {
    let epoch = harness
        .scenario()
        .self_advance_epoch(timeout)
        .await
        .expect("advance the epoch");
    assert_eq!(epoch, expected, "expected epoch {expected}");
    epoch
}

/// Wait until the nodes list exactly `expected` tracks on a tape, and return them
async fn wait_tracks(client: &Client, tape: &Address, expected: usize) -> Vec<CompressedTrack> {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    let mut last = None;
    loop {
        if let Ok((tracks, _)) = client.list_tracks_by_tape(tape, None, 16).await {
            if tracks.len() == expected {
                return tracks;
            }
            last = Some(tracks.len());
        }
        assert!(Instant::now() < deadline, "tape {tape} lists {last:?} tracks, expected {expected}");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// Wait until the nodes answer not-found for a track by its address
async fn wait_gone(client: &Client, track: Address) {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    loop {
        match client.get_track(&track).await {
            Err(TapedriveError::NotFound) => return,
            other => assert!(Instant::now() < deadline, "track {track} is not gone: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// Read an object by name, retrying while its track and slices reach the nodes
async fn read_object(client: &Client, tape: &Address, name: &str) -> Vec<u8> {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    loop {
        match client.get_object(tape, name).await {
            Ok(data) => return data,
            Err(error) => assert!(Instant::now() < deadline, "read {name} timed out: {error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}
