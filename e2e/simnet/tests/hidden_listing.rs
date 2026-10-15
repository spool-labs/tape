use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::tape::TapeFlags;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, TestGateway, run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;

const TARGET_GROUPS: u64 = 1;
const GATEWAY_HEALTH_TIMEOUT: Duration = Duration::from_secs(180);
const HIDDEN_KEY: &str = "private/ledger.txt";
const PUBLIC_KEY: &str = "public/readme.txt";
const ACCESS_KEY_ID: &str = "AKIAEXAMPLES3GATEWAY";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SIGV4_REGION: &str = "us-east-1";
const SIGV4_SERVICE: &str = "s3";
const SIGNED_HEADERS: &str = "host;x-amz-content-sha256;x-amz-date";

// the gateway lists a hidden bucket only to a signed caller, and a public bucket to anyone
#[test]
fn hidden_listing() {
    run_simnet_test(hidden_listing_inner);
}

async fn hidden_listing_inner() {
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

    let hidden = TapeKey::generate();
    let public = TapeKey::generate();
    {
        let scenario = harness.scenario();
        let owner = scenario.sdk(harness.admin());
        owner
            .reserve_with_flags(&hidden, StorageUnits::mb(1), 4, TapeFlags::HIDDEN)
            .await
            .expect("reserve hidden bucket");
        owner
            .put_object(&hidden, HIDDEN_KEY, b"hidden ledger", Some("text/plain"))
            .await
            .expect("write to the hidden bucket");
        owner
            .reserve(&public, StorageUnits::mb(1), 4)
            .await
            .expect("reserve public bucket");
        owner
            .put_object(&public, PUBLIC_KEY, b"public readme", Some("text/plain"))
            .await
            .expect("write to the public bucket");
    }

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

    let s3_base = gateway.s3_base_url();
    let host = s3_base.trim_start_matches("http://").to_string();
    let client = reqwest::Client::new();
    let hidden_path = format!("/{}", hidden.address());
    let public_path = format!("/{}", public.address());

    let signed = wait_listed(&client, &s3_base, &host, &hidden_path, HIDDEN_KEY).await;
    assert_eq!(signed, StatusCode::OK, "a signed caller should list the hidden bucket");

    for query in ["?list-type=2", ""] {
        let url = format!("{s3_base}{hidden_path}{query}");
        let anonymous = client.get(&url).send().await.expect("anonymous list");
        let status = anonymous.status();
        let body = anonymous.text().await.expect("list body");
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "an unsigned list{query} of a hidden bucket should be refused"
        );
        assert!(!body.contains(HIDDEN_KEY), "a refused list should hold no keys");
    }

    let url = format!("{s3_base}{public_path}?list-type=2");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let response = client.get(&url).send().await.expect("public list");
        assert_eq!(response.status(), StatusCode::OK, "anyone should list a public bucket");
        if response.text().await.expect("list body").contains(PUBLIC_KEY) {
            break;
        }
        assert!(Instant::now() < deadline, "the public bucket should list its object");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop runtimes");
}

/// Poll a signed ListObjectsV2 until the gateway has ingested the key, then report its answer
async fn wait_listed(
    client: &reqwest::Client,
    s3_base: &str,
    host: &str,
    path: &str,
    key: &str,
) -> StatusCode {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let response = client
            .get(format!("{s3_base}{path}?list-type=2"))
            .headers(sigv4_headers("GET", host, path, "list-type=2"))
            .send()
            .await
            .expect("signed list");
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status != StatusCode::OK || body.contains(key) || Instant::now() >= deadline {
            return status;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// SigV4 headers for a bodiless request, signed with the gateway's bootstrap credential
fn sigv4_headers(method: &str, host: &str, path: &str, query: &str) -> HeaderMap {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64;
    let amz_date = format_amz_datetime(now);
    let date_stamp = &amz_date[..8];
    let scope = format!("{date_stamp}/{SIGV4_REGION}/{SIGV4_SERVICE}/aws4_request");
    let payload_hash = sha256_hex(b"");

    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{SIGNED_HEADERS}\n{payload_hash}");
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
