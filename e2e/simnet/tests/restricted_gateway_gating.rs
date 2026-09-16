use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::grant::{ReadGrant, ReadRequest};
use tape_core::track::types::TrackVisibility;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TestGateway, run_simnet_test};
use tape_protocol::api::{
    encode_read_expiry, encode_read_grant, encode_read_signature, read_request_headers,
    READ_EXPIRY_HEADER, READ_GRANT_HEADER, READ_SIGNATURE_HEADER,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
const GATEWAY_HEALTH_TIMEOUT: Duration = Duration::from_secs(180);
const GRANT_TTL: Duration = Duration::from_secs(3600);
const REQUEST_TTL: Duration = Duration::from_secs(60);

/// A gateway serves a restricted object only to a caller it can name: an
/// unsigned native read and an unsigned S3 read are both refused, and the
/// native routes open for a reader presenting a grant and a signature over the
/// call it makes.
#[test]
fn restricted_object_gateway_gating() {
    run_simnet_test(restricted_object_gateway_gating_inner);
}

async fn restricted_object_gateway_gating_inner() {
    peer_tls::install_default_provider();

    let node_count = GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    gateway.enable_s3().expect("enable s3 listener");

    let all: Vec<usize> = (0..node_count).collect();
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
            .wait_nodes_healthy(Duration::from_secs(30))
            .await
            .expect("nodes healthy");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active");
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 2");
    }

    let bucket = TapeKey::generate();
    let name = "restricted/gated.txt";
    let payload = vec![0x77u8; 32 * 1024];
    let track_address;
    let gateway_grant;
    let reader = CryptoKeypair::new(&mut rand::thread_rng());
    let reader_grant;
    {
        let scenario = harness.scenario();
        let owner = scenario.sdk(harness.admin());
        owner
            .reserve(&bucket, StorageUnits::mb(1), 4)
            .await
            .expect("reserve bucket tape");

        let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
            visibility: TrackVisibility::Restricted,
            ..WriteOptions::default()
        });
        track_address = writer
            .put_object(&bucket, name, &payload, Some("text/plain"))
            .await
            .expect("restricted write");

        // The gateway fetches slices under a tape-wide grant naming its own mTLS
        // identity; the reader holds one naming its own key.
        gateway_grant = owner
            .issue_tape_read_grant(&bucket, gateway.tls_pubkey(), GRANT_TTL)
            .await
            .expect("issue gateway grant");
        reader_grant = owner
            .issue_read_grant(
                &bucket,
                hash(name.as_bytes()),
                NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
                GRANT_TTL,
            )
            .await
            .expect("issue reader grant");
    }

    gateway.set_grants(vec![encode_read_grant(&gateway_grant)]);

    {
        let scenario = harness.scenario();
        scenario
            .register_gateway(&gateway, BasisPoints(100))
            .await
            .expect("register gateway");
        scenario
            .stake_gateway(&gateway, 2_000)
            .await
            .expect("stake gateway");
        // A node learns the gateway from the registration block, so its peer
        // cache cannot hold it before ingest reaches that slot.
        harness
            .wait_ingest_current(active_timeout)
            .await
            .expect("nodes ingested the gateway registration");
        harness
            .wait_gateway_known(&gateway, active_timeout)
            .await
            .expect("nodes learned gateway peer");
    }

    gateway.start().await.expect("start gateway");
    gateway
        .wait_healthy(GATEWAY_HEALTH_TIMEOUT)
        .await
        .expect("gateway healthy");

    let native = format!("{}/object/{}", gateway.base_url(), track_address);
    assert_eq!(
        wait_status(&native, Duration::from_secs(120)).await,
        StatusCode::FORBIDDEN,
        "the native listener should refuse a restricted object"
    );

    let anonymous = format!(
        "{}/{}/{}",
        gateway.s3_base_url(),
        bucket.address(),
        name
    );
    assert_eq!(
        wait_status(&anonymous, Duration::from_secs(120)).await,
        StatusCode::FORBIDDEN,
        "an unsigned s3 read of a restricted object should be refused"
    );

    for path in [
        format!("/object/{track_address}"),
        format!("/track/{track_address}"),
    ] {
        let url = format!("{}{path}", gateway.base_url());
        let client = reqwest::Client::new();

        let grant_only = client
            .get(&url)
            .header(READ_GRANT_HEADER, encode_read_grant(&reader_grant))
            .send()
            .await
            .expect("grant-only read");
        assert_eq!(
            grant_only.status(),
            StatusCode::FORBIDDEN,
            "a grant with no signature should be refused on {path}"
        );

        let stranger = CryptoKeypair::new(&mut rand::thread_rng());
        let borrowed = client
            .get(&url)
            .headers(signed_headers(&reader_grant, &stranger, &path, now_secs() + 30))
            .send()
            .await
            .expect("wrong-key read");
        assert_eq!(
            borrowed.status(),
            StatusCode::FORBIDDEN,
            "a signature by another key should be refused on {path}"
        );

        let stale = client
            .get(&url)
            .headers(signed_headers(&reader_grant, &reader, &path, now_secs() - 30))
            .send()
            .await
            .expect("expired read");
        assert_eq!(
            stale.status(),
            StatusCode::FORBIDDEN,
            "an expired request should be refused on {path}"
        );

        let mut headers = HeaderMap::new();
        for (name, value) in
            read_request_headers(&reader_grant, &reader, "GET", &path, REQUEST_TTL)
        {
            headers.insert(
                HeaderName::from_static(name),
                HeaderValue::from_str(&value).expect("header value"),
            );
        }
        let opened = client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .expect("granted read");
        assert_eq!(
            opened.status(),
            StatusCode::OK,
            "a signed grant should open {path}"
        );
        assert_eq!(
            opened.bytes().await.expect("body").as_ref(),
            payload.as_slice(),
            "{path} should serve the object's bytes"
        );
    }

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop runtimes");
}

/// The three reader headers, signed by whatever key and expiry the caller names.
fn signed_headers(
    grant: &ReadGrant,
    signer: &CryptoKeypair,
    path: &str,
    expiry: i64,
) -> HeaderMap {
    let signature = ReadRequest::sign(signer, "GET", path, expiry);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static(READ_GRANT_HEADER),
        HeaderValue::from_str(&encode_read_grant(grant)).expect("grant header"),
    );
    headers.insert(
        HeaderName::from_static(READ_EXPIRY_HEADER),
        HeaderValue::from_str(&encode_read_expiry(expiry)).expect("expiry header"),
    );
    headers.insert(
        HeaderName::from_static(READ_SIGNATURE_HEADER),
        HeaderValue::from_str(&encode_read_signature(&signature)).expect("signature header"),
    );

    headers
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

/// Poll until the gateway has ingested the object, then report its answer.

async fn wait_status(url: &str, timeout: Duration) -> StatusCode {
    let client = reqwest::Client::new();
    let start = Instant::now();
    loop {
        let status = client
            .get(url)
            .send()
            .await
            .map(|response| response.status())
            .unwrap_or(StatusCode::BAD_GATEWAY);
        if status != StatusCode::NOT_FOUND || start.elapsed() >= timeout {
            return status;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

