use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::TrackVisibility;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_privacy::envelope::binding::EnvelopeBinding;
use tape_privacy::errors::PrivacyError;
use tape_privacy::key_source::KeySource;
use tape_privacy::recipient::Recipient;
use tape_privacy::sealed::{is_envelope, keyring_name, open, seal, share};
use tape_privacy::wrap::keyring::Keyring;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::read_options::ReadOptions;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
const GRANT_TTL: Duration = Duration::from_secs(3600);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(120);
const OCTETS: &str = "application/octet-stream";

/// The composition the privacy crate documents: seal client-side, store the
/// ciphertext restricted, store the keyring as a public sidecar, and open with
/// a wallet the keyring names. Grants decide who fetches, wraps decide who reads.
#[test]
fn encrypted_object_flow() {
    run_simnet_test(encrypted_object_flow_inner);
}

async fn encrypted_object_flow_inner() {
    let node_count = GROUP_SIZE;
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
    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");

    let owner = scenario.sdk(harness.admin());
    let bucket = TapeKey::generate();
    owner
        .reserve(&bucket, StorageUnits::mb(4), 4)
        .await
        .expect("reserve bucket tape");
    let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
        visibility: TrackVisibility::Restricted,
        ..WriteOptions::default()
    });

    let name = "encrypted/notes.txt";
    let sidecar = keyring_name(name.as_bytes());
    let sidecar_name = String::from_utf8(sidecar.clone()).expect("keyring name is utf-8");
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i * 31 + 7) as u8).collect();
    let binding = EnvelopeBinding::for_name(bucket.address().to_bytes(), name.as_bytes());

    let owner_wallet = wallet(bucket.keypair());
    let reader_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let reader_wallet = wallet(&reader_identity);
    let sealed = seal(
        &binding,
        &data,
        &[
            Recipient::Wallet(owner_wallet.verifying_key()),
            Recipient::Wallet(reader_wallet.verifying_key()),
        ],
    )
    .expect("seal the payload");
    assert!(is_envelope(&sealed.ciphertext), "the stored form is an envelope");
    assert_ne!(sealed.ciphertext, data, "the stored form is not the plaintext");

    // The ciphertext is restricted, the keyring is an ordinary public track.
    writer
        .put_object(&bucket, name, &sealed.ciphertext, Some(OCTETS))
        .await
        .expect("restricted write of the ciphertext");
    owner
        .put_object(&bucket, &sidecar_name, &sealed.keyring.pack(), Some(OCTETS))
        .await
        .expect("public write of the keyring");
    let payload_key = hash(name.as_bytes());
    let keyring_key = hash(&sidecar);
    wait_track_visible(&owner, &bucket.address(), payload_key, Duration::from_secs(60)).await;
    wait_track_visible(&owner, &bucket.address(), keyring_key, Duration::from_secs(60)).await;

    // Without a grant the ciphertext is out of reach; the keyring is public.
    assert!(owner.get_object(&bucket.address(), name).await.is_err(), "ungranted fetch refused");
    let public_keyring = owner
        .get_object(&bucket.address(), &sidecar_name)
        .await
        .expect("keyring readable without a grant");
    assert_eq!(public_keyring, sealed.keyring.pack());

    let reader_tls = NetworkTlsPubkey::new(reader_identity.pubkey().to_bytes());
    let grant = owner
        .issue_read_grant(&bucket, payload_key, reader_tls, GRANT_TTL)
        .await
        .expect("issue read grant");
    let reader = scenario
        .reader_sdk(reader_identity.clone())
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(grant),
            ..ReadOptions::default()
        });

    // Grant plus wrap opens the object.
    let fetched = reader
        .get_object(&bucket.address(), name)
        .await
        .expect("granted fetch of the ciphertext");
    assert_eq!(fetched, sealed.ciphertext, "nodes hand back the envelope untouched");
    let keyring = Keyring::unpack(&public_keyring).expect("unpack the keyring");
    let opened = open(&binding, &fetched, &keyring, &KeySource::Wallet(reader_wallet))
        .expect("open with the reader wallet");
    assert_eq!(opened, data, "encrypted object round-trips");

    // A grant without a wrap fetches bytes it cannot read.
    let blind_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let blind_wallet = wallet(&blind_identity);
    let blind_grant = owner
        .issue_read_grant(
            &bucket,
            payload_key,
            NetworkTlsPubkey::new(blind_identity.pubkey().to_bytes()),
            GRANT_TTL,
        )
        .await
        .expect("issue blind grant");
    let blind = scenario
        .reader_sdk(blind_identity)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(blind_grant),
            ..ReadOptions::default()
        });
    let blind_bytes = blind
        .get_object(&bucket.address(), name)
        .await
        .expect("a granted reader fetches the envelope");
    let unopened = open(&binding, &blind_bytes, &keyring, &KeySource::Wallet(blind_wallet));
    assert!(
        matches!(unopened, Err(PrivacyError::NoContentKey)),
        "a wallet the keyring does not name cannot open, got {unopened:?}"
    );

    // Sharing rewrites only the sidecar; the payload track is untouched.
    let stranger_identity = Arc::new(CryptoKeypair::new(&mut rand::thread_rng()));
    let stranger_wallet = wallet(&stranger_identity);
    let extended = share(
        &keyring,
        &KeySource::Wallet(owner_wallet),
        &Recipient::Wallet(stranger_wallet.verifying_key()),
    )
    .expect("share to the stranger");
    let keyring_v1 = track_number(&owner, &bucket.address(), keyring_key).await;
    owner
        .put_object(&bucket, &sidecar_name, &extended.pack(), Some(OCTETS))
        .await
        .expect("put over the keyring");
    harness
        .wait_ingest_current(VISIBILITY_TIMEOUT)
        .await
        .expect("nodes ingested the keyring put-over");
    wait_track_advanced(&owner, &bucket.address(), keyring_key, keyring_v1, VISIBILITY_TIMEOUT).await;

    let stranger_grant = owner
        .issue_read_grant(
            &bucket,
            payload_key,
            NetworkTlsPubkey::new(stranger_identity.pubkey().to_bytes()),
            GRANT_TTL,
        )
        .await
        .expect("issue stranger grant");
    let stranger = scenario
        .reader_sdk(stranger_identity)
        .expect("reader sdk")
        .with_read_options(ReadOptions {
            grant: Some(stranger_grant),
            ..ReadOptions::default()
        });
    let stranger_bytes = stranger
        .get_object(&bucket.address(), name)
        .await
        .expect("stranger fetches the envelope");
    assert_eq!(stranger_bytes, sealed.ciphertext, "the payload track did not move");
    let shared_keyring = Keyring::unpack(
        &stranger
            .get_object(&bucket.address(), &sidecar_name)
            .await
            .expect("stranger fetches the keyring"),
    )
    .expect("unpack the shared keyring");
    let reopened = open(&binding, &stranger_bytes, &shared_keyring, &KeySource::Wallet(stranger_wallet))
        .expect("open with the shared wallet");
    assert_eq!(reopened, data, "the shared recipient reads the object");

    harness.stop_all().await.expect("stop runtimes");
}

/// The same ed25519 key as a privacy-crate wallet
fn wallet(keypair: &CryptoKeypair) -> SigningKey {
    SigningKey::from_keypair_bytes(&keypair.to_keypair_bytes()).expect("ed25519 keypair bytes")
}

/// A write returns once slices are held and certification confirms, which can be
/// before every node has replayed the registration block; reads by name need the
/// nodes to have caught up, so tests wait for the track to become findable.
async fn wait_track_visible(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if client
            .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "track was not visible on nodes within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Latest version the nodes report for a name.
async fn track_number(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
) -> tape_core::types::TrackNumber {
    client
        .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
        .await
        .expect("track visible before the put-over")
        .track_number
}

/// A put-over is visible when the answering node's latest pointer has moved
/// past the version it held before.
async fn wait_track_advanced(
    client: &tape_sdk::tapedrive::Tapedrive<rpc_litesvm::LiteSvmRpc, peer_http::HttpApi>,
    tape: &tape_crypto::prelude::Address,
    key: tape_crypto::hash::Hash,
    previous: tape_core::types::TrackNumber,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(track) = client
            .find_track(tape, key, tape_protocol::api::FindTrackVersion::Latest)
            .await
        {
            if track.track_number > previous {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nodes still report {previous:?} for the put-over after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
