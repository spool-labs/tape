//! Live end-to-end test for the gateway admission seam: a funded-balance
//! implementation injected through the public embedding API gates real
//! SigV4-signed S3 writes against a LiteSVM chain.
//!
//! Mirrors the proven `s3_gateway.rs` boot sequence but keeps the write policy
//! itself permissive (`enable_s3_writes`: bootstrap credential, default-allow,
//! no admin plane), so the injected admission implementation is the only gate
//! left standing between an authorized request and the write. The test then
//! walks the ticket lifecycle end to end:
//!
//! - **unfunded** — a signed PutObject is denied `403 AccessDenied` by the
//!   admission gate alone, and the budget-caps reservation granted just before
//!   the gate is released (nothing outstanding in the gateway ledger),
//! - **funded** — the same signed PutObject succeeds as a real delegate-signed
//!   on-chain write, the implementation observes commit at the actual byte
//!   count, and the balance is debited by exactly that amount,
//! - **refund** — a signed DeleteObject of a missing key authorizes, resolves
//!   to a no-op, and refunds its ticket (balance restored),
//! - **per-op tickets** — a multipart create / two parts / abort sequence
//!   yields one distinct ticket per operation, all settled.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::header::{CONTENT_TYPE, ETAG};
use reqwest::StatusCode;
use sha2::{Digest, Sha256};

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, TestGateway, run_simnet_test,
};
use tape_gateway::admission::{Admission, AdmissionDeny, AdmissionRequest, WriteOp};
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::LedgerOps;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;
/// Epochs the bucket tape is reserved for; generous so it stays unexpired
/// through the activation sequence and the on-chain precondition checks.
const RESERVE_EPOCHS: u64 = 32;

/// Object key for the funded / unfunded PutObject phases.
const PUT_KEY: &str = "uploads/funded.txt";
const PUT_CONTENT_TYPE: &str = "text/plain";
/// Size of the PutObject body (matches the proven coded path in s3_gateway.rs).
const OBJECT_SIZE_BYTES: usize = 64 * 1024;
/// Size of each buffered multipart part in the per-op ticket phase.
const PART_SIZE_BYTES: usize = 16 * 1024;
/// Funded balance credited before the successful write phases: enough for the
/// 64 KiB put plus both parts, with headroom that must survive untouched.
const FUNDED_BALANCE_BYTES: u64 = 256 * 1024;

/// Bootstrap SigV4 credential the gateway is configured with. It carries no
/// store credential, so the resolved principal is the default owner authority.
const ACCESS_KEY_ID: &str = "AKIAEXAMPLEADMISSION";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SIGV4_REGION: &str = "us-east-1";
const SIGV4_SERVICE: &str = "s3";

/// What the funded-balance implementation saw and holds. One lock for the
/// balance, the outstanding holds, and the observed lifecycle events.
#[derive(Default)]
struct FundedLedger {
    balance: u64,
    holds: HashMap<u64, u64>,
    reserves: Vec<(u64, WriteOp, u64)>,
    commits: Vec<(u64, u64)>,
    refunds: Vec<u64>,
}

/// A minimal prepaid in-memory admission implementation: reserve holds the
/// byte estimate against the balance, commit charges the actual bytes and
/// releases the hold, refund releases the hold untouched.
#[derive(Default)]
struct FundedAdmission {
    ledger: Mutex<FundedLedger>,
}

impl FundedAdmission {
    fn credit(&self, amount: u64) {
        self.ledger.lock().expect("admission ledger lock").balance += amount;
    }

    fn snapshot(&self) -> FundedLedger {
        let ledger = self.ledger.lock().expect("admission ledger lock");
        FundedLedger {
            balance: ledger.balance,
            holds: ledger.holds.clone(),
            reserves: ledger.reserves.clone(),
            commits: ledger.commits.clone(),
            refunds: ledger.refunds.clone(),
        }
    }
}

#[async_trait]
impl Admission for FundedAdmission {
    async fn reserve(&self, request: AdmissionRequest) -> Result<(), AdmissionDeny> {
        let mut ledger = self.ledger.lock().expect("admission ledger lock");
        ledger
            .reserves
            .push((request.ticket, request.op, request.estimated_bytes));
        if ledger.balance < request.estimated_bytes {
            return Err(AdmissionDeny {
                reason: "funded balance is too low for this write".to_string(),
                retry_after_seconds: None,
            });
        }
        ledger.balance -= request.estimated_bytes;
        ledger.holds.insert(request.ticket, request.estimated_bytes);
        Ok(())
    }

    fn commit(&self, ticket: u64, actual_bytes: u64) {
        let mut ledger = self.ledger.lock().expect("admission ledger lock");
        if let Some(held) = ledger.holds.remove(&ticket) {
            // The hold was the estimate; charge the actual and release the rest
            ledger.balance += held.saturating_sub(actual_bytes);
        }
        ledger.commits.push((ticket, actual_bytes));
    }

    fn refund(&self, ticket: u64) {
        let mut ledger = self.ledger.lock().expect("admission ledger lock");
        if let Some(held) = ledger.holds.remove(&ticket) {
            ledger.balance += held;
        }
        ledger.refunds.push(ticket);
    }
}

// a funded-balance admission implementation gates real signed S3 writes
#[test]
fn funded_writes() {
    run_simnet_test(funded_writes_inner);
}

async fn funded_writes_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    let s3_addr = gateway
        .enable_s3_writes(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
        .expect("enable s3 write path");

    // The custom gate under test, injected through the public embedding seam
    // (TestGateway passes it into run_with_context). The test keeps its own
    // handle to fund the balance and read back what the gateway drove.
    let admission = Arc::new(FundedAdmission::default());
    gateway.set_admission(admission.clone());
    eprintln!("s3_admission: fixtures built, s3 listener on {s3_addr}");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register storage nodes");
        scenario
            .stake_all(STORAGE_NODE_STAKE)
            .await
            .expect("stake storage nodes");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
        eprintln!("s3_admission: network started");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start storage nodes");

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .expect("storage nodes healthy");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active");

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
            .expect("storage nodes active at epoch 3");
        eprintln!("s3_admission: storage nodes active at epoch 3");
    }

    // Reserve the bucket tape and delegate its writes to the gateway keypair so
    // the on-chain precondition holds for the signed writes below.
    let put_body = deterministic_bytes(OBJECT_SIZE_BYTES);
    let tape_key = TapeKey::generate();
    let bucket = tape_key.address();
    let gateway_delegate = Address::from(gateway.authority());
    {
        let scenario = harness.scenario();
        let writer = scenario.sdk(harness.admin());
        let reserve_capacity =
            StorageUnits::from_bytes(put_body.len() as u64) + StorageUnits::mb(8);
        writer
            .reserve(&tape_key, reserve_capacity, RESERVE_EPOCHS)
            .await
            .expect("reserve bucket tape");
        writer
            .set_tape_delegate(&tape_key, gateway_delegate)
            .await
            .expect("delegate bucket writes to the gateway keypair");
        eprintln!("s3_admission: bucket {bucket} reserved and delegated to {gateway_delegate}");
    }

    // The proven gateway activation sequence from s3_gateway.rs, so the booted
    // gateway is a recognized staked peer and its delegate-signed writes land.
    {
        let scenario = harness.scenario();
        scenario
            .register_gateway(&gateway, BasisPoints(100))
            .await
            .expect("register gateway");
        scenario
            .stake_gateway(&gateway, GATEWAY_STAKE)
            .await
            .expect("stake gateway");
        wait_gateway_known_by_storage_nodes(&harness, &gateway, active_timeout)
            .await
            .expect("storage nodes learned gateway peer");

        gateway.start().await.expect("start gateway");
        wait_gateway_healthy(&gateway.base_url(), Duration::from_secs(180))
            .await
            .expect("gateway healthy");
        eprintln!("s3_admission: gateway runtime healthy");

        let epoch4 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 4");
        assert!(epoch4 >= 4, "expected at least epoch 4, got {epoch4}");
        let epoch5 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance beyond epoch 4");
        assert!(epoch5 > epoch4, "expected epoch beyond {epoch4}, got {epoch5}");

        scenario
            .advance_gateway_pool_ok(&gateway)
            .await
            .expect("advance gateway pool");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active after gateway activation");
        eprintln!("s3_admission: gateway pool advanced, nodes active");
    }

    let s3_base = gateway.s3_base_url();
    let host = s3_host(&s3_base);
    let bucket_label = bucket.to_string();
    // The bootstrap credential carries no store record, so the chokepoint
    // resolves it to the default owner authority; both the caps ledger and the
    // admission gate see that principal.
    let principal = Address::default();

    // ----- unfunded: the admission gate alone denies the write -------------

    let denied =
        signed_put_response(&s3_base, &host, &bucket_label, PUT_KEY, &put_body, PUT_CONTENT_TYPE)
            .await;
    let status = denied.status();
    let body = denied.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "unfunded PutObject should be denied with 403, got {status}: {body}"
    );
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "a hard admission deny should map to AccessDenied, got: {body}"
    );
    assert!(
        body.contains("funded balance is too low"),
        "the deny should surface the implementation's reason, got: {body}"
    );

    let seen = admission.snapshot();
    assert_eq!(seen.reserves.len(), 1, "one write should have hit the gate");
    let (_, denied_op, denied_estimate) = seen.reserves[0];
    assert_eq!(denied_op, WriteOp::Put);
    assert_eq!(denied_estimate, put_body.len() as u64);
    assert!(seen.commits.is_empty(), "a denied reserve must never settle");
    assert!(seen.refunds.is_empty(), "a denied reserve must never settle");
    assert!(seen.holds.is_empty(), "a denied reserve must hold nothing");

    // The caps reservation granted just before the gate must have been
    // released: nothing outstanding in the gateway's own budget ledger.
    let ledger_entry = gateway
        .context()
        .store
        .get_ledger(&principal)
        .expect("read gateway budget ledger");
    assert_eq!(ledger_entry.writes_reserved, 0, "caps reservation must be released on deny");
    assert_eq!(ledger_entry.bytes_reserved, 0, "caps reservation must be released on deny");
    assert_eq!(ledger_entry.sol_reserved, 0, "caps reservation must be released on deny");
    assert_eq!(ledger_entry.bytes_committed, 0, "a denied write must not bill the caps ledger");
    eprintln!("s3_admission: unfunded PutObject denied 403, caps reservation released");

    // ----- funded: the same write succeeds and settles at actual cost ------

    admission.credit(FUNDED_BALANCE_BYTES);
    let etag = assert_s3_signed_put(
        &s3_base,
        &host,
        &bucket_label,
        PUT_KEY,
        &put_body,
        PUT_CONTENT_TYPE,
    )
    .await;
    eprintln!("s3_admission: funded PutObject succeeded (etag {etag})");

    let seen = admission.snapshot();
    assert_eq!(seen.reserves.len(), 2, "the funded put should be the second reserve");
    let (put_ticket, put_op, put_estimate) = seen.reserves[1];
    assert_eq!(put_op, WriteOp::Put);
    assert_eq!(put_estimate, put_body.len() as u64);
    assert_eq!(
        seen.commits,
        vec![(put_ticket, put_body.len() as u64)],
        "commit must settle the put's ticket at the actual byte count"
    );
    assert!(seen.holds.is_empty(), "a committed ticket must not stay held");
    assert_eq!(
        seen.balance,
        FUNDED_BALANCE_BYTES - put_body.len() as u64,
        "the balance must be debited by exactly the actual bytes"
    );

    let ledger_entry = gateway
        .context()
        .store
        .get_ledger(&principal)
        .expect("read gateway budget ledger");
    assert_eq!(ledger_entry.bytes_reserved, 0, "the caps reservation must be settled");
    assert_eq!(
        ledger_entry.bytes_committed,
        put_body.len() as u64,
        "the caps ledger must bill the committed write"
    );

    // ----- refund: a no-op delete releases its ticket untouched ------------

    let missing = signed_delete_response(&s3_base, &host, &bucket_label, "uploads/missing.txt").await;
    assert_eq!(
        missing.status(),
        StatusCode::NO_CONTENT,
        "deleting a missing key is an idempotent S3 success"
    );

    let seen = admission.snapshot();
    assert_eq!(seen.reserves.len(), 3, "the delete should be the third reserve");
    let (delete_ticket, delete_op, _) = seen.reserves[2];
    assert_eq!(delete_op, WriteOp::Delete);
    assert_eq!(
        seen.refunds,
        vec![delete_ticket],
        "a no-op delete must refund its ticket"
    );
    assert_eq!(
        seen.balance,
        FUNDED_BALANCE_BYTES - put_body.len() as u64,
        "a refunded ticket must leave the balance untouched"
    );
    eprintln!("s3_admission: no-op DeleteObject refunded its ticket");

    // ----- per-op tickets: multipart create / parts / abort ----------------

    let multipart_key = "uploads/multipart.bin";
    let part_body = deterministic_bytes(PART_SIZE_BYTES);
    let upload_id =
        s3_create_multipart(&s3_base, &host, &bucket_label, multipart_key, PUT_CONTENT_TYPE).await;
    s3_upload_part(&s3_base, &host, &bucket_label, multipart_key, &upload_id, 1, &part_body).await;
    s3_upload_part(&s3_base, &host, &bucket_label, multipart_key, &upload_id, 2, &part_body).await;
    s3_abort_multipart(&s3_base, &host, &bucket_label, multipart_key, &upload_id).await;

    let seen = admission.snapshot();
    assert_eq!(
        seen.reserves.len(),
        7,
        "create, two parts, and abort should each hit the gate once"
    );
    let mut tickets: Vec<u64> = Vec::new();
    for (ticket, _, _) in &seen.reserves {
        assert!(!tickets.contains(ticket), "every reserve must get a fresh ticket");
        tickets.push(*ticket);
    }
    let (_, create_op, _) = seen.reserves[3];
    let (_, part_op, part_estimate) = seen.reserves[4];
    let (_, abort_op, _) = seen.reserves[6];
    assert_eq!(create_op, WriteOp::CreateMultipart);
    assert_eq!(part_op, WriteOp::UploadPart);
    assert_eq!(part_estimate, part_body.len() as u64);
    assert_eq!(abort_op, WriteOp::Abort);
    assert!(seen.holds.is_empty(), "every multipart ticket must be settled");
    assert_eq!(
        seen.balance,
        FUNDED_BALANCE_BYTES - put_body.len() as u64 - 2 * part_body.len() as u64,
        "each buffered part must debit its actual bytes"
    );
    eprintln!("s3_admission: multipart ops each carried their own settled ticket");

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop storage nodes");
}

// SigV4 request signing below mirrors s3_gateway.rs; duplication across e2e
// test files is accepted here.

fn s3_host(s3_base: &str) -> String {
    s3_base
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

/// The signed headers carried on every request, lowercase and already sorted.
const SIGNED_HEADERS: &str = "host;x-amz-content-sha256;x-amz-date";

/// Build the SigV4 auth headers for a request, mirroring the gateway's
/// canonical request assembly exactly. Returns `(authorization, x-amz-date,
/// x-amz-content-sha256)`.
fn sigv4_headers(method: &str, host: &str, path: &str, body: &[u8]) -> (String, String, String) {
    sigv4_headers_with_payload(method, host, path, "", &sha256_hex(body))
}

/// Canonicalize a raw query string the way the gateway does (split, sort by
/// key, bare flags as `key=`). The multipart params are alphanumeric, so no
/// percent-encoding is needed.
fn canonical_query(raw_query: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in raw_query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        pairs.push((key.to_string(), value.to_string()));
    }
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// As `sigv4_headers` but with an explicit canonical query string and payload
/// hash, so multipart requests can sign their query.
fn sigv4_headers_with_payload(
    method: &str,
    host: &str,
    path: &str,
    canonical_query: &str,
    payload_hash: &str,
) -> (String, String, String) {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64;
    let amz_date = format_amz_datetime(now_secs);
    let date_stamp = amz_date[..8].to_string();
    let scope = format!("{date_stamp}/{SIGV4_REGION}/{SIGV4_SERVICE}/aws4_request");

    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{SIGNED_HEADERS}\n{payload_hash}"
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key =
        derive_signing_key(SECRET_ACCESS_KEY, &date_stamp, SIGV4_REGION, SIGV4_SERVICE);
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY_ID}/{scope}, \
         SignedHeaders={SIGNED_HEADERS}, Signature={signature}"
    );
    (authorization, amz_date, payload_hash.to_string())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn derive_signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Format unix seconds as the AWS basic-UTC timestamp.
fn format_amz_datetime(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hour, minute, second) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Send a SigV4-signed `PUT /{bucket}/{key}` and return the raw response.
async fn signed_put_response(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    content_type: &str,
) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let url = format!("{base}{path}");
    let (authorization, amz_date, payload_hash) = sigv4_headers("PUT", host, &path, body);

    client
        .put(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .header(CONTENT_TYPE, content_type)
        .body(body.to_vec())
        .send()
        .await
        .expect("signed put send")
}

/// Send a SigV4-signed `DELETE /{bucket}/{key}` and return the raw response.
async fn signed_delete_response(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let url = format!("{base}{path}");
    let (authorization, amz_date, payload_hash) = sigv4_headers("DELETE", host, &path, b"");

    client
        .delete(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .send()
        .await
        .expect("signed delete send")
}

/// Assert a SigV4-signed `PUT /{bucket}/{key}` succeeds and return its ETag.
async fn assert_s3_signed_put(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    content_type: &str,
) -> String {
    let response = signed_put_response(base, host, bucket, key, body, content_type).await;
    let status = response.status();
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        panic!("signed PutObject should return 200, got {status}: {body}");
    }
    etag.expect("signed PutObject response should include an ETag")
}

/// Extract the text content of the first `<tag>...</tag>` in an XML body.
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// CreateMultipartUpload; returns the upload id.
async fn s3_create_multipart(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    content_type: &str,
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let raw_query = "uploads";
    let url = format!("{base}{path}?{raw_query}");
    let (authorization, amz_date, payload_hash) =
        sigv4_headers_with_payload("POST", host, &path, &canonical_query(raw_query), &sha256_hex(b""));

    let response = client
        .post(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .header(CONTENT_TYPE, content_type)
        .send()
        .await
        .expect("create multipart send");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::OK,
        "CreateMultipartUpload should return 200, got {status}: {body}"
    );
    extract_xml_tag(&body, "UploadId").expect("CreateMultipartUpload should return an UploadId")
}

/// UploadPart; returns the part ETag the gateway minted.
async fn s3_upload_part(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    body: &[u8],
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let raw_query = format!("partNumber={part_number}&uploadId={upload_id}");
    let url = format!("{base}{path}?{raw_query}");
    let (authorization, amz_date, payload_hash) =
        sigv4_headers_with_payload("PUT", host, &path, &canonical_query(&raw_query), &sha256_hex(body));

    let response = client
        .put(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .body(body.to_vec())
        .send()
        .await
        .expect("upload part send");
    let status = response.status();
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        panic!("UploadPart should return 200, got {status}: {body}");
    }
    etag.expect("UploadPart response should include an ETag")
}

/// AbortMultipartUpload; asserts the `204 No Content` success.
async fn s3_abort_multipart(base: &str, host: &str, bucket: &str, key: &str, upload_id: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let raw_query = format!("uploadId={upload_id}");
    let url = format!("{base}{path}?{raw_query}");
    let (authorization, amz_date, payload_hash) =
        sigv4_headers_with_payload("DELETE", host, &path, &canonical_query(&raw_query), &sha256_hex(b""));

    let response = client
        .delete(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .send()
        .await
        .expect("abort multipart send");
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "AbortMultipartUpload should return 204"
    );
}

/// Wait until every running storage node's peer manager knows the gateway.
async fn wait_gateway_known_by_storage_nodes(
    harness: &SimnetHarness,
    gateway: &TestGateway,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let tls_pubkey = gateway.tls_pubkey();

    loop {
        let mut running = 0usize;
        let mut known = 0usize;
        for node in harness.nodes().iter().filter(|node| node.is_running()) {
            running += 1;
            if node
                .context()
                .peer_manager
                .peer_for_tls_pubkey(tls_pubkey)
                .is_some()
            {
                known += 1;
            }
        }

        if running > 0 && known == running {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            anyhow::bail!(
                "timed out waiting for storage nodes to learn gateway peer, known {known}/{running}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll the gateway's native health endpoint until it reports `200 OK`.
async fn wait_gateway_healthy(base: &str, timeout: Duration) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let start = Instant::now();
    loop {
        if let Ok(response) = client.get(format!("{base}/v1/health")).send().await {
            if response.status() == StatusCode::OK {
                return Ok(());
            }
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("timed out waiting for gateway health");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Deterministic pseudo-random bytes, matching gateway_read.rs.
fn deterministic_bytes(len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        let mixed = index.wrapping_mul(31) ^ index.rotate_left(5);
        bytes.push(mixed as u8);
    }
    bytes
}
