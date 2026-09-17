use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::share::{unix_now, ReadRequest, ShareToken, ALL_TRACKS};
use tape_core::track::types::TrackVisibility;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TestGateway, run_simnet_test};
use tape_protocol::api::{
    encode_read_expiry, encode_read_signature, encode_share_token, read_request_headers,
    READ_EXPIRY_HEADER, READ_SIGNATURE_HEADER, SHARE_TOKEN_HEADER,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::write_options::WriteOptions;

const TARGET_GROUPS: u64 = 1;
const GATEWAY_HEALTH_TIMEOUT: Duration = Duration::from_secs(180);
const SHARE_TTL: Duration = Duration::from_secs(3600);
const REQUEST_TTL: Duration = Duration::from_secs(60);

// the gateway refuses unsigned native and S3 reads of a hidden object, and opens native routes to a reader who signs the call
#[test]
fn hidden_gateway() {
    run_simnet_test(hidden_gateway_inner);
}

async fn hidden_gateway_inner() {
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
    let name = "hidden/gated.txt";
    let payload = vec![0x77u8; 32 * 1024];
    let track_address;
    let gateway_token;
    let reader = CryptoKeypair::new(&mut rand::thread_rng());
    let reader_token;
    {
        let scenario = harness.scenario();
        let owner = scenario.sdk(harness.admin());
        owner
            .reserve(&bucket, StorageUnits::mb(1), 4)
            .await
            .expect("reserve bucket tape");

        let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
            visibility: TrackVisibility::Hidden,
            ..WriteOptions::default()
        });
        track_address = writer
            .put_object(&bucket, name, &payload, Some("text/plain"))
            .await
            .expect("hidden write");

        // The gateway fetches slices with an all-tracks token for its own mTLS
        // identity; the reader holds a token for its own key.
        gateway_token = owner
            .share(&bucket, ALL_TRACKS, gateway.tls_pubkey(), SHARE_TTL)
            .await
            .expect("share with the gateway");
        reader_token = owner
            .share(
                &bucket,
                hash(name.as_bytes()),
                NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
                SHARE_TTL,
            )
            .await
            .expect("share with the reader");
    }

    gateway.set_share_tokens(vec![encode_share_token(&gateway_token)]);

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
        "the native listener should refuse a hidden object"
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
        "an unsigned s3 read of a hidden object should be refused"
    );

    for path in [
        format!("/object/{track_address}"),
        format!("/track/{track_address}"),
    ] {
        let url = format!("{}{path}", gateway.base_url());
        let client = reqwest::Client::new();

        let token_only = client
            .get(&url)
            .header(SHARE_TOKEN_HEADER, encode_share_token(&reader_token))
            .send()
            .await
            .expect("token-only read");
        assert_eq!(
            token_only.status(),
            StatusCode::FORBIDDEN,
            "a share token with no signature should be refused on {path}"
        );

        let stranger = CryptoKeypair::new(&mut rand::thread_rng());
        let borrowed = client
            .get(&url)
            .headers(signed_headers(&reader_token, &stranger, &path, unix_now() + 30))
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
            .headers(signed_headers(&reader_token, &reader, &path, unix_now() - 30))
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
            read_request_headers(&reader_token, &reader, "GET", &path, REQUEST_TTL)
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
            .expect("signed read");
        assert_eq!(
            opened.status(),
            StatusCode::OK,
            "a signed share token should open {path}"
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

/// The three reader headers, signed with the given key and expiry
fn signed_headers(
    token: &ShareToken,
    signer: &CryptoKeypair,
    path: &str,
    expiry: i64,
) -> HeaderMap {
    let signature = ReadRequest::sign(signer, "GET", path, expiry);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static(SHARE_TOKEN_HEADER),
        HeaderValue::from_str(&encode_share_token(token)).expect("token header"),
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

/// Poll until the gateway has ingested the object, then report its answer
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
