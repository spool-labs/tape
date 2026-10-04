// An immutable track can't be deleted by its tape's owner. The flag comes from
// the write, or from a tape reserved immutable, which puts it on every track
// written there. The second half reserves a hidden, immutable tape and writes
// to it with a client that sets no flag.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tape_api::errors::TapeError;
use tape_api::instruction::build_delete_track_ix;
use tape_api::program::tapedrive::TrackAddress;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::share::ALL_TRACKS;
use tape_core::tape::TapeFlags;
use tape_core::track::types::{CompressedTrack, TrackFlags};
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, ContentType, StorageUnits};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, run_simnet_test};
use tape_peer_http::HttpApi;
use tape_protocol::api::FindTrackVersion;
use rpc_litesvm::LiteSvmRpc;
use tape_rpc::RpcError;
use tape_rpc_client::parse_tape_error;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::read_options::ReadOptions;
use tape_sdk::tapedrive::Tapedrive;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
// Simnet epochs advance on wall-clock, so the tapes are reserved well past the run.
const EPOCHS: u64 = 40;
const SHARE_TTL: Duration = Duration::from_secs(3600);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);

type Client = Tapedrive<LiteSvmRpc, HttpApi>;

// immutable inline, coded and stream objects refuse every delete, and an immutable tape makes its tracks the same
#[test]
fn immutable_tracks() {
    run_simnet_test(immutable_tracks_inner);
}

async fn immutable_tracks_inner() {
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

    let owner = scenario.sdk(harness.admin());
    let immutable = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
        flags: TrackFlags::IMMUTABLE,
        ..WriteOptions::default()
    });

    let tiny = b"one ledger line".to_vec();
    let page = vec![0x5Au8; 64 * 1024];
    let book = vec![0xA5u8; 96 * 1024];
    let draft = vec![0x11u8; 32 * 1024];

    // A plain tape: the flag comes from each write.
    let bucket = TapeKey::generate();
    let tape = bucket.address();
    owner
        .reserve(&bucket, StorageUnits::mb(8), EPOCHS)
        .await
        .expect("reserve plain tape");

    let inline = immutable
        .put_object(&bucket, "ledger/tiny.txt", &tiny, Some("text/plain"))
        .await
        .expect("immutable inline write");
    let coded = immutable
        .put_object(&bucket, "ledger/page.bin", &page, None)
        .await
        .expect("immutable coded write");
    let stream = immutable
        .write_named_bytes(&bucket, "ledger/book.bin", ContentType::Unknown, &book)
        .await
        .expect("immutable stream write");
    let plain = owner
        .put_object(&bucket, "notes/draft.bin", &draft, None)
        .await
        .expect("plain write");

    // inline, coded, one stream chunk, the stream's manifest and the plain track
    let tracks = wait_tracks(&owner, &tape, 5).await;
    let locked: Vec<Address> = tracks
        .iter()
        .filter(|track| track.is_immutable())
        .map(|track| track.address())
        .collect();
    assert_eq!(locked.len(), 4, "every track of the immutable writes carries the flag");
    assert!(locked.contains(&inline) && locked.contains(&coded) && locked.contains(&stream.manifest));
    assert!(!locked.contains(&plain), "the plain write stays deletable");

    assert_eq!(read_object(&owner, &tape, "ledger/tiny.txt").await, tiny);
    assert_eq!(read_object(&owner, &tape, "ledger/page.bin").await, page);
    assert_eq!(read_stream(&owner, &stream.manifest).await, book);

    // The SDK refuses each delete before it sends anything.
    for track in &locked {
        let deleted = owner.delete(&bucket, *track).await;
        assert!(matches!(deleted, Err(TapedriveError::Immutable)), "delete of {track}: {deleted:?}");
    }
    let by_name = owner.delete_object(&bucket, "ledger/page.bin").await;
    assert!(matches!(by_name, Err(TapedriveError::Immutable)), "delete by name: {by_name:?}");
    let whole = owner.delete_object_as(&bucket, stream.manifest).await;
    assert!(matches!(whole, Err(TapedriveError::Immutable)), "stream delete: {whole:?}");

    // The program refuses the same delete from a client that skips that check.
    for track in &locked {
        let refused = raw_delete(&owner, &bucket, *track).await.expect_err("program refuses");
        assert_eq!(parse_tape_error(&refused), Some(TapeError::TrackImmutable), "{refused}");
    }

    // A plain track on the same tape still deletes, and the immutable ones stay.
    owner.delete(&bucket, plain).await.expect("delete the plain track");
    let left = wait_tracks(&owner, &tape, 4).await;
    assert!(left.iter().all(CompressedTrack::is_immutable));
    assert_eq!(read_object(&owner, &tape, "ledger/page.bin").await, page);
    assert_eq!(read_stream(&owner, &stream.manifest).await, book);

    // A hidden, immutable tape: the writer sets no flag and the tape adds both.
    let vault = TapeKey::generate();
    let vault_tape = vault.address();
    let tape_flags = TapeFlags::HIDDEN | TapeFlags::IMMUTABLE;
    let reserved = owner
        .reserve_with_flags(&vault, StorageUnits::mb(8), EPOCHS, tape_flags)
        .await
        .expect("reserve hidden immutable tape");
    assert_eq!(reserved.flags, tape_flags);

    let vault_page = owner
        .put_object(&vault, "vault/page.bin", &page, None)
        .await
        .expect("coded write to the vault");
    let vault_line = owner
        .put_object(&vault, "vault/line.txt", &tiny, Some("text/plain"))
        .await
        .expect("inline write to the vault");
    let vault_book = owner
        .write_named_bytes(&vault, "vault/book.bin", ContentType::Unknown, &book)
        .await
        .expect("stream write to the vault");
    // coded, inline, one stream chunk and the stream's manifest
    let vault_tracks = wait_tracks(&owner, &vault_tape, 4).await;
    for track in &vault_tracks {
        assert!(track.is_hidden() && track.is_immutable() && track.is_certified(), "{track:?}");
    }

    for track in [vault_page, vault_line, vault_book.manifest] {
        let deleted = owner.delete(&vault, track).await;
        assert!(matches!(deleted, Err(TapedriveError::Immutable)), "delete of {track}: {deleted:?}");
        let refused = raw_delete(&owner, &vault, track).await.expect_err("program refuses");
        assert_eq!(parse_tape_error(&refused), Some(TapeError::TrackImmutable), "{refused}");
    }

    // The tape's hidden flag gates the bytes the way a hidden write does.
    assert!(owner.get_object(&vault_tape, "vault/page.bin").await.is_err());
    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let token = owner
        .share(&vault, ALL_TRACKS, reader_tls, SHARE_TTL)
        .await
        .expect("share the vault");
    let reader = scenario
        .reader_sdk(reader_identity)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            share_token: Some(token),
            ..ReadOptions::default()
        });
    assert_eq!(read_object(&reader, &vault_tape, "vault/page.bin").await, page);
    assert_eq!(read_object(&reader, &vault_tape, "vault/line.txt").await, tiny);
    assert_eq!(read_stream(&reader, &vault_book.manifest).await, book);

    // A resumed write computes its content address with the tape's flags, so it
    // finds the track the tape already holds.
    let resuming = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
        resume: true,
        ..WriteOptions::default()
    });
    let again = resuming
        .write_named(&vault, "vault/page.bin", ContentType::Unknown, &page, EPOCHS)
        .await
        .expect("resume a finished write");
    assert_eq!(again.address(), vault_page);
    let book_again = resuming
        .write_named_bytes(&vault, "vault/book.bin", ContentType::Unknown, &book)
        .await
        .expect("resume a finished stream");
    assert_eq!(book_again.manifest, vault_book.manifest);
    wait_tracks(&owner, &vault_tape, 4).await;

    // An immutable upload that stopped after its register can't be deleted, and
    // a resumed write finishes it.
    let late = vec![0x3Cu8; 48 * 1024];
    let (registered, _plan) = owner
        .write_named_blob(&vault, "vault/late.bin", ContentType::Unknown, &late)
        .await
        .expect("register only");
    assert!(registered.track.is_immutable() && registered.track.is_registered());
    wait_tracks(&owner, &vault_tape, 5).await;
    let deleted = owner.delete(&vault, registered.address).await;
    assert!(matches!(deleted, Err(TapedriveError::Immutable)), "registered delete: {deleted:?}");

    let finished = resuming
        .write_named(&vault, "vault/late.bin", ContentType::Unknown, &late, EPOCHS)
        .await
        .expect("resume the registered write");
    assert!(finished.is_certified());
    assert_eq!(finished.address(), registered.address);
    wait_tracks(&owner, &vault_tape, 5).await;
    assert_eq!(read_object(&reader, &vault_tape, "vault/late.bin").await, late);

    // A write under a name the tape holds lands as a new track, the name moves
    // to it, and the old track stays where it was.
    let second = vec![0xC3u8; 40 * 1024];
    let moved = owner
        .put_object(&vault, "vault/page.bin", &second, None)
        .await
        .expect("write over the name");
    assert_ne!(moved, vault_page);
    wait_tracks(&owner, &vault_tape, 6).await;
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the second write");
    let latest = owner
        .find_track(&vault_tape, hash(b"vault/page.bin"), FindTrackVersion::Latest)
        .await
        .expect("name resolves");
    assert_eq!(latest.address(), moved);
    let old = owner.get_track(&vault_page).await.expect("old track stays");
    assert!(old.is_immutable() && old.is_certified());
    assert_eq!(read_object(&reader, &vault_tape, "vault/page.bin").await, second);

    harness.stop_all().await.expect("stop runtimes");
}

/// Send a delete straight to the program, past the SDK's own check
async fn raw_delete(client: &Client, bucket: &TapeKey, track: Address) -> Result<(), RpcError> {
    let payer = client.payer().expect("payer");
    let tape_signer = bucket.keypair();
    let proof = client.get_track_proof(&track).await.expect("track proof");
    let ix = build_delete_track_ix(payer.pubkey().into(), bucket.pubkey().into(), proof);

    client
        .rpc()
        .send_instructions_with_signers(payer, vec![ix], &[tape_signer])
        .await
        .map(|_| ())
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

/// Read a stream by its manifest, retrying while its slices reach the nodes
async fn read_stream(client: &Client, manifest: &Address) -> Vec<u8> {
    let deadline = Instant::now() + VISIBILITY_TIMEOUT;
    loop {
        match client.read_bytes(manifest).await {
            Ok(data) => return data,
            Err(error) => assert!(Instant::now() < deadline, "read {manifest} timed out: {error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}
