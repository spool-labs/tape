use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, IF_MODIFIED_SINCE};
use sha2::{Digest, Sha256};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::share::{unix_now, ReadRequest, ShareToken, ALL_TRACKS};
use tape_core::tape::TapeFlags;
use tape_core::track::types::TrackFlags;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::hash::hash;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, TestGateway, run_simnet_test,
};
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

/// The key a signed S3 put writes to the hidden tape, read while the write queue still holds it
const QUEUED_KEY: &str = "queued/note.txt";
const ACCESS_KEY_ID: &str = "AKIAEXAMPLES3GATEWAY";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SIGV4_REGION: &str = "us-east-1";
const SIGV4_SERVICE: &str = "s3";
const SIGNED_HEADERS: &str = "host;x-amz-content-sha256;x-amz-date";

// the gateway refuses unsigned native and S3 reads of a hidden object, queued or landed, and opens native routes to a reader who signs the call
#[test]
fn hidden_gateway() {
    run_simnet_test(hidden_gateway_inner);
}

async fn hidden_gateway_inner() {
    tape_peer_tls::install_default_provider();

    let node_count = TAPENET_GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    gateway
        .enable_s3_writes(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
        .expect("enable s3 listener");

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
    let hidden_bucket = TapeKey::generate();
    let queued_token;
    {
        let scenario = harness.scenario();
        let owner = scenario.sdk(harness.admin());
        owner
            .reserve(&bucket, StorageUnits::mb(1), 4)
            .await
            .expect("reserve bucket tape");

        let writer = scenario.sdk(harness.admin()).with_write_options(WriteOptions {
            flags: TrackFlags::HIDDEN,
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

        // Every track on this tape lands hidden, and the gateway writes to it as delegate.
        owner
            .reserve_with_flags(&hidden_bucket, StorageUnits::mb(1), 4, TapeFlags::HIDDEN)
            .await
            .expect("reserve hidden bucket tape");
        owner
            .set_tape_delegate(&hidden_bucket, Address::from(gateway.authority()))
            .await
            .expect("delegate hidden bucket writes to the gateway");
        queued_token = owner
            .share(
                &hidden_bucket,
                hash(QUEUED_KEY.as_bytes()),
                NetworkTlsPubkey::new(reader.pubkey().to_bytes()),
                SHARE_TTL,
            )
            .await
            .expect("share the queued object with the reader");
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

    let hidden_bucket = hidden_bucket.address().to_string();
    assert_queued_hidden(&gateway, &hidden_bucket, &queued_token, &reader).await;

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop runtimes");
}

/// Put an object to a hidden tape through S3, then read it while the write queue holds it
async fn assert_queued_hidden(
    gateway: &TestGateway,
    bucket: &str,
    token: &ShareToken,
    reader: &CryptoKeypair,
) {
    let client = reqwest::Client::new();
    let s3_base = gateway.s3_base_url();
    let host = s3_base.trim_start_matches("http://").to_string();
    let path = format!("/{bucket}/{QUEUED_KEY}");
    let s3_url = format!("{s3_base}{path}");
    let payload = vec![0x5au8; 16 * 1024];

    let put = client
        .put(&s3_url)
        .headers(sigv4_headers("PUT", &host, &path, &payload))
        .header(CONTENT_TYPE, "text/plain")
        .body(payload.clone())
        .send()
        .await
        .expect("signed put");
    assert_eq!(put.status(), StatusCode::OK, "a signed put to a hidden tape should queue");

    // The drain ticks every 500 ms and a write takes slots to land, so these reads find the queue.
    let get = client.get(&s3_url).send().await.expect("anonymous get");
    assert_eq!(
        get.status(),
        StatusCode::FORBIDDEN,
        "an unsigned s3 get of a queued hidden object should be refused"
    );

    let head = client.head(&s3_url).send().await.expect("anonymous head");
    assert_eq!(
        head.status(),
        StatusCode::FORBIDDEN,
        "an unsigned s3 head of a queued hidden object should be refused"
    );

    let conditional = client
        .get(&s3_url)
        .header(IF_MODIFIED_SINCE, "Fri, 01 Jan 2100 00:00:00 GMT")
        .send()
        .await
        .expect("anonymous conditional get");
    assert_eq!(
        conditional.status(),
        StatusCode::FORBIDDEN,
        "an unsigned conditional get of a queued hidden object should be refused before a 304"
    );

    let native_path = format!("/bucket{path}");
    let native_url = format!("{}{native_path}", gateway.base_url());
    let native = client.get(&native_url).send().await.expect("unsigned native read");
    assert_eq!(
        native.status(),
        StatusCode::FORBIDDEN,
        "an unsigned native read of a queued hidden object should be refused"
    );

    let opened = client
        .get(&native_url)
        .headers(signed_headers(token, reader, &native_path, unix_now() + 30))
        .send()
        .await
        .expect("signed native read");
    assert_eq!(
        opened.status(),
        StatusCode::OK,
        "a share token for the object's name should open the queued object"
    );
    assert_eq!(opened.bytes().await.expect("body").as_ref(), payload.as_slice());

    let signed = client
        .get(&s3_url)
        .headers(sigv4_headers("GET", &host, &path, b""))
        .send()
        .await
        .expect("signed s3 get");
    assert_eq!(
        signed.status(),
        StatusCode::OK,
        "a signed s3 get should open the queued object"
    );
    assert_eq!(signed.bytes().await.expect("body").as_ref(), payload.as_slice());
}

/// SigV4 headers for a request with no query, signed with the gateway's bootstrap credential
fn sigv4_headers(method: &str, host: &str, path: &str, body: &[u8]) -> HeaderMap {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64;
    let amz_date = format_amz_datetime(now);
    let date_stamp = &amz_date[..8];
    let scope = format!("{date_stamp}/{SIGV4_REGION}/{SIGV4_SERVICE}/aws4_request");
    let payload_hash = sha256_hex(body);

    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let canonical_request =
        format!("{method}\n{path}\n\n{canonical_headers}\n{SIGNED_HEADERS}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let mut key = hmac_sha256(format!("AWS4{SECRET_ACCESS_KEY}").as_bytes(), date_stamp.as_bytes());
    for part in [SIGV4_REGION, SIGV4_SERVICE, "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY_ID}/{scope}, \
         SignedHeaders={SIGNED_HEADERS}, Signature={signature}"
    );

    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("authorization", authorization),
        ("x-amz-date", amz_date.clone()),
        ("x-amz-content-sha256", payload_hash),
    ] {
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(&value).expect("sigv4 header"),
        );
    }

    headers
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Unix seconds as the AWS `YYYYMMDDTHHMMSSZ` timestamp
fn format_amz_datetime(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hour, minute, second) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);

    // Howard Hinnant's civil_from_days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);

    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
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
