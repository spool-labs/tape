//! Live end-to-end test for the S3-compatible gateway: anonymous reads plus the
//! fail-closed, admin-gated, delegate-signed write path.
//!
//! Mirrors `gateway_read.rs` (the proven gateway boot/stake/serve sequence) but
//! enables the `[gateway.s3]` listener together with the write-authorization
//! control plane (`enable_s3_admin_writes`) and exercises both surfaces against a
//! real LiteSVM chain.
//!
//! Configuration (fail-closed): the bucket tape delegates writes to the gateway's
//! keypair (`set_tape_delegate`), and the gateway is configured with that same
//! key as its S3 delegate signer plus:
//! - a bootstrap SigV4 credential (so a signature can be *verified*),
//! - `write.default = Deny` (a verified signature is *authorized* only when a
//!   stored policy rule allows it),
//! - a server pepper (so the admin API can issue credentials), and
//! - an operator bearer token (so the admin control-plane listener starts).
//!
//! Reads (anonymous, unsigned): a named 64 KiB object is written to the reserved
//! tape via the owner SDK, then read back through the gateway's S3 listener with
//! ListObjectsV2 / GetObject / HeadObject and the NoSuchKey error path.
//!
//! Writes (AWS SigV4-signed), end to end through the live gateway and admin API:
//! - an **anonymous** PutObject is rejected `403 AccessDenied` (writes require a
//!   signature),
//! - a **SigV4-signed** PutObject with **no issued credential** is rejected `403`
//!   (default-deny: a verified signature with no store credential and no policy
//!   rule is not authorized),
//! - via the **admin API** (operator bearer token) a credential scoped to the
//!   tape is issued and a policy rule allows it; the same SigV4-signed PutObject
//!   now **authorizes** and performs a **real delegate-signed on-chain
//!   `TrackWrite`**, which is confirmed (a) network-wide via the owner SDK listing
//!   and (b) by reading the bytes back through the gateway's S3 GET (after the
//!   gateway ingests + certifies the write),
//! - the credential is **revoked** via the admin API and the same signed
//!   PutObject is again rejected `403`,
//! - the global write **kill switch** is engaged via the admin API and a signed
//!   PutObject (with the credential re-issued and active) is denied.
//!
//! The SigV4 request signer is implemented in-test (see the `sigv4` helpers
//! below) and mirrors the gateway's own canonicalization in
//! `network/gateway/src/http/handlers/s3/sigv4.rs`, so no `aws-sigv4` dependency
//! is pulled in.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use reqwest::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use solana_signer::Signer;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, TestGateway, run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::object::ListObjectsQuery;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;
/// Epochs the bucket tape is reserved for. Generous so it stays unexpired through
/// the multi-epoch activation sequence and the on-chain write precondition check
/// (`tape.expiry_epoch > current_epoch`) when the signed writes run.
const RESERVE_EPOCHS: u64 = 32;

/// Object key (with a `/` so it also exercises the wildcard `{*key}` route and a
/// non-empty list prefix). Written by the owner SDK as a 64 KiB coded object and
/// read back through the gateway.
const OBJECT_KEY: &str = "photos/cat.jpg";
const OBJECT_PREFIX: &str = "photos/";
/// Content type written with the object and asserted on GET/HEAD. `text/plain`
/// maps to the `ContentType::TextPlain` -> `text/plain` header.
const OBJECT_CONTENT_TYPE: &str = "text/plain";

/// Object key written through the gateway's S3 surface with a SigV4-signed
/// PutObject (a real delegate-signed on-chain write). Used across every write
/// phase — anonymous (denied), signed-without-credential (denied), authorized
/// (written + read back), revoked (denied), and kill-switched (denied) — so the
/// "same signed PutObject" flips on the authorization state alone.
const PUT_KEY: &str = "uploads/note.txt";
const PUT_PREFIX: &str = "uploads/";
const PUT_CONTENT_TYPE: &str = "text/plain";

/// Size of both coded objects written in this test (the owner-SDK named object and
/// the SigV4 PutObject body). 64 KiB matches the proven coded path in `gateway_read.rs`.
const OBJECT_SIZE_BYTES: usize = 64 * 1024;

/// Bootstrap SigV4 credential the gateway is configured with. A request signed
/// with this access key id / secret *verifies* against the gateway; whether it is
/// *authorized* is then governed by the store credential + policy (default-deny).
/// The admin-issued store credential is keyed by this same access key id (the
/// SigV4 layer can only verify against the configured bootstrap secret).
const ACCESS_KEY_ID: &str = "AKIAEXAMPLES3GATEWAY";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SIGV4_REGION: &str = "us-east-1";
const SIGV4_SERVICE: &str = "s3";

/// Operator bearer token authenticating the admin control plane.
const OPERATOR_TOKEN: &str = "s3-operator-secret-token";
/// Server pepper for credential-secret hashing (`HMAC-SHA256(secret, pepper)`).
const SERVER_PEPPER: &str = "s3-server-pepper";

/// Policy rule key (priority, id) for the allow rule that admits the credential's
/// principal to write the bucket under default-deny.
const POLICY_PRIORITY: u32 = 10;
const POLICY_ID: u64 = 1;

// anonymous S3 reads plus the fail-closed, admin-gated, delegate-signed write path
#[test]
fn read_write() {
    run_simnet_test(read_write_inner);
}

async fn read_write_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    // Enable the S3 listener, the delegate-signed write path, AND the
    // fail-closed write-authorization admin control plane: the gateway's own
    // keypair becomes the S3 delegate signer, with a bootstrap SigV4 credential,
    // `write.default = Deny`, a server pepper, and an operator-token-guarded admin
    // listener.
    let (s3_addr, admin_addr) = gateway
        .enable_s3_admin_writes(ACCESS_KEY_ID, SECRET_ACCESS_KEY, OPERATOR_TOKEN, SERVER_PEPPER)
        .expect("enable s3 admin write path");
    eprintln!("s3_gateway: fixtures built, s3 listener on {s3_addr}, admin on {admin_addr}");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        eprintln!("s3_gateway: system initialized");
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
        eprintln!("s3_gateway: network started");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start storage nodes");
    eprintln!("s3_gateway: storage runtimes started");

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
        eprintln!("s3_gateway: storage nodes active at epoch 3");
    }

    // Reserve the bucket tape, write the 64 KiB coded object as the owner, and
    // point the tape's on-chain `delegate` at the gateway's keypair so the
    // gateway can later sign writes for it as a delegate.
    let object_data = deterministic_bytes(OBJECT_SIZE_BYTES);
    // The SigV4-signed write below lands a real coded+certified track so it can be
    // read back through the gateway's S3 GET (the read path requires the track to
    // be certified).
    let put_body = deterministic_bytes(OBJECT_SIZE_BYTES);
    let tape_key = TapeKey::generate();
    let bucket = tape_key.address();
    let gateway_delegate = Address::from(gateway.authority());
    // The owner authority the issued credential acts on behalf of: the tape's
    // on-chain authority (the admin keypair that reserved it). This is the
    // `principal` recorded in the credential and matched by the policy allow rule.
    let principal = Address::from(harness.admin().pubkey());
    {
        let scenario = harness.scenario();
        let writer = scenario.sdk(harness.admin());
        let reserve_capacity = StorageUnits::from_bytes((object_data.len() + put_body.len()) as u64)
            + StorageUnits::mb(8);
        writer
            .reserve(&tape_key, reserve_capacity, RESERVE_EPOCHS)
            .await
            .expect("reserve bucket tape");
        eprintln!("s3_gateway: bucket tape {bucket} reserved");
        writer
            .put_object(&tape_key, OBJECT_KEY, &object_data, Some(OBJECT_CONTENT_TYPE))
            .await
            .expect("put named object");
        eprintln!("s3_gateway: named object written + certified");

        // Confirm the object is visible (ingested + certified) via the SDK list
        // against the storage nodes before booting the gateway.
        wait_sdk_object_listed(&harness, &bucket, OBJECT_PREFIX, OBJECT_KEY, active_timeout).await;
        eprintln!("s3_gateway: named object visible via SDK list");

        // Delegate writes on this tape to the gateway's keypair. After this the
        // tape's on-chain `delegate` equals the gateway's S3 delegate signer, so
        // the on-chain write precondition (`tape.delegate == our_delegate`) holds.
        writer
            .set_tape_delegate(&tape_key, gateway_delegate)
            .await
            .expect("delegate bucket writes to the gateway keypair");
        eprintln!("s3_gateway: bucket delegated writes to gateway {gateway_delegate}");
    }

    // Register, stake, and boot the gateway with the S3 listener enabled. This is
    // the proven `gateway_read.rs` activation sequence so the gateway becomes a
    // recognized staked peer that can pull slices from (and resolve proofs on) the
    // storage nodes.
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
        eprintln!("s3_gateway: storage nodes learned gateway peer");

        gateway.start().await.expect("start gateway");
        wait_gateway_healthy(&gateway.base_url(), Duration::from_secs(180))
            .await
            .expect("gateway healthy");
        eprintln!("s3_gateway: gateway runtime healthy");

        let epoch4 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 4");
        assert!(epoch4 >= 4, "expected at least epoch 4, got {epoch4}");
        let epoch5 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 5");
        assert!(epoch5 > epoch4, "expected epoch beyond {epoch4}, got {epoch5}");

        scenario
            .advance_gateway_pool_ok(&gateway)
            .await
            .expect("advance gateway pool");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active after gateway activation");
        eprintln!("s3_gateway: gateway pool advanced, nodes active");
    }

    let s3_base = gateway.s3_base_url();
    let admin_base = gateway.s3_admin_base_url();
    let host = s3_host(&s3_base);
    let bucket_label = bucket.to_string();
    let principal_label = principal.to_string();

    // ----- READ surface (anonymous / unsigned) -----------------------------

    // Wait until the 64 KiB object is fully readable through the gateway's S3
    // surface (the gateway ingested the object-list entry + track-certify event).
    wait_s3_head_ok(&s3_base, &bucket_label, OBJECT_KEY, Duration::from_secs(180)).await;
    eprintln!("s3_gateway: object readable via S3 HEAD");

    assert_s3_list_objects_v2(&s3_base, &bucket_label, OBJECT_KEY).await;
    eprintln!("s3_gateway: ListObjectsV2 assertion passed");

    let get_etag = assert_s3_get_object(
        &s3_base,
        &bucket_label,
        OBJECT_KEY,
        &object_data,
        OBJECT_CONTENT_TYPE,
    )
    .await;
    eprintln!("s3_gateway: GetObject assertion passed");

    let head_etag = assert_s3_head_object(
        &s3_base,
        &bucket_label,
        OBJECT_KEY,
        object_data.len(),
        OBJECT_CONTENT_TYPE,
    )
    .await;
    assert_eq!(
        get_etag, head_etag,
        "GET and HEAD must report the same ETag for the same object"
    );
    eprintln!("s3_gateway: HeadObject assertion passed");

    assert_s3_no_such_key(&s3_base, &bucket_label, "missing/object.bin").await;
    eprintln!("s3_gateway: NoSuchKey assertion passed");

    // ----- WRITE surface: fail-closed authorization ------------------------

    // The admin control plane comes up alongside the data plane; wait for it.
    wait_admin_healthy(&admin_base, OPERATOR_TOKEN, Duration::from_secs(60)).await;
    eprintln!("s3_gateway: admin control plane healthy");

    // (1a) An *unsigned* write is rejected: anonymous reads are public, but writes
    // require a signature (rejected at the SigV4 gate, before authorization).
    assert_s3_unsigned_put_forbidden(&s3_base, &bucket_label, PUT_KEY).await;
    eprintln!("s3_gateway: unsigned PutObject correctly rejected (403 AccessDenied)");

    // (1b) A *signed* write with NO issued credential is rejected: the signature
    // verifies (bootstrap credential), but with `write.default = Deny` and no
    // store credential / policy rule the authorization chokepoint denies it.
    let denied = signed_put_response(&s3_base, &host, &bucket_label, PUT_KEY, &put_body, PUT_CONTENT_TYPE).await;
    assert_access_denied(denied, "signed PutObject with no issued credential").await;
    eprintln!("s3_gateway: signed PutObject with no credential correctly rejected (403)");

    // (2) Issue a credential scoped to the bucket and a policy rule allowing the
    // principal to write it. After this the same signed request authorizes.
    admin_issue_credential(&admin_base, OPERATOR_TOKEN, &principal_label, &bucket_label).await;
    admin_create_policy_rule(&admin_base, OPERATOR_TOKEN, &principal_label, &bucket_label).await;
    eprintln!("s3_gateway: credential issued + policy allow rule created via admin API");

    // ListBuckets is credential-scoped: a signed request lists exactly the bucket
    // the credential is scoped to; an anonymous request is denied (account op).
    assert_s3_list_buckets(&s3_base, &host, &bucket_label).await;
    assert_s3_list_buckets_anonymous_denied(&s3_base).await;
    eprintln!("s3_gateway: ListBuckets listed the scoped bucket (signed) and denied anonymous");

    let put_etag = assert_s3_signed_put(
        &s3_base,
        &host,
        &bucket_label,
        PUT_KEY,
        &put_body,
        PUT_CONTENT_TYPE,
    )
    .await;
    assert!(
        put_etag.starts_with('"') && put_etag.ends_with('"') && put_etag.len() > 2,
        "PutObject ETag should be quoted and non-empty, got {put_etag}"
    );
    eprintln!("s3_gateway: authorized SigV4 PutObject succeeded (200, etag {put_etag})");

    // The delegate-signed write must land on-chain and be indexed network-wide:
    // confirm it via the owner SDK listing (served by the storage nodes), which is
    // independent of the gateway's own ingestion.
    wait_sdk_object_listed(&harness, &bucket, PUT_PREFIX, PUT_KEY, active_timeout).await;
    eprintln!("s3_gateway: SigV4-written object visible via SDK list (on-chain write confirmed)");

    // ... and the gateway, once it ingests + certifies its own write, serves the
    // exact bytes back through the public S3 read surface.
    wait_s3_head_ok(&s3_base, &bucket_label, PUT_KEY, Duration::from_secs(180)).await;
    let read_back_etag =
        assert_s3_get_object(&s3_base, &bucket_label, PUT_KEY, &put_body, PUT_CONTENT_TYPE).await;
    eprintln!(
        "s3_gateway: delegate-signed object read back through gateway GET (etag {read_back_etag})"
    );

    // HeadBucket returns 200 for the existing bucket tape; a single-track object
    // serves a `206 Partial Content` slice for an HTTP `Range` request.
    assert_s3_head_bucket(&s3_base, &bucket_label).await;
    assert_s3_get_range(&s3_base, &bucket_label, PUT_KEY, &put_body, 0, 3).await;
    eprintln!("s3_gateway: HeadBucket 200 + ranged GET 206 verified");

    // (2b) A streamed UNSIGNED-PAYLOAD PutObject takes the bounded-memory write
    // path (object_reader -> write_object_stream -> chunk track + manifest) instead
    // of buffering + hash-verifying, and must read back identically through GET.
    let stream_key = "uploads/streamed.bin";
    let stream_body = deterministic_bytes(48 * 1024);
    let stream_etag =
        assert_s3_streamed_put(&s3_base, &host, &bucket_label, stream_key, &stream_body, PUT_CONTENT_TYPE)
            .await;
    eprintln!("s3_gateway: streamed (UNSIGNED-PAYLOAD) PutObject succeeded (200, etag {stream_etag})");
    wait_s3_head_ok(&s3_base, &bucket_label, stream_key, Duration::from_secs(180)).await;
    let stream_read_etag =
        assert_s3_get_object(&s3_base, &bucket_label, stream_key, &stream_body, PUT_CONTENT_TYPE).await;
    eprintln!("s3_gateway: streamed object read back through gateway GET (etag {stream_read_etag})");
    // A ranged GET of the manifest-backed object serves through the stream
    // range path (chunk-subset decode) on the S3 listener.
    assert_s3_get_range(&s3_base, &bucket_label, stream_key, &stream_body, 1000, 2023).await;
    eprintln!("s3_gateway: streamed object ranged GET returned 206");

    // (2c) A store-backed (durable) multipart upload round-trips on-chain:
    // create -> upload one part -> complete -> read back through GET.
    let mp_key = "uploads/multipart.bin";
    let mp_body = deterministic_bytes(40 * 1024);
    let upload_id = s3_create_multipart(&s3_base, &host, &bucket_label, mp_key, PUT_CONTENT_TYPE).await;
    let part_etag = s3_upload_part(&s3_base, &host, &bucket_label, mp_key, &upload_id, 1, &mp_body).await;
    let complete_etag =
        s3_complete_multipart(&s3_base, &host, &bucket_label, mp_key, &upload_id, &[(1, part_etag)]).await;
    eprintln!("s3_gateway: multipart upload completed (200, etag {complete_etag})");
    wait_s3_head_ok(&s3_base, &bucket_label, mp_key, Duration::from_secs(180)).await;
    let mp_read_etag =
        assert_s3_get_object(&s3_base, &bucket_label, mp_key, &mp_body, PUT_CONTENT_TYPE).await;
    eprintln!("s3_gateway: multipart object read back through gateway GET (etag {mp_read_etag})");

    // ListObjects V1 (legacy `GET /{bucket}` without `list-type=2`) lists the
    // bucket's objects in the V1 `<Marker>` wire shape, including a known key.
    assert_s3_list_objects_v1(&s3_base, &bucket_label, mp_key).await;
    eprintln!("s3_gateway: ListObjects V1 listed the bucket (Marker shape) including {mp_key}");

    // (3) Revoke the credential; the same signed PutObject is now denied (the
    // credential resolves but is no longer usable — step 3 of the chokepoint).
    admin_revoke_credential(&admin_base, OPERATOR_TOKEN).await;
    let denied = signed_put_response(&s3_base, &host, &bucket_label, PUT_KEY, &put_body, PUT_CONTENT_TYPE).await;
    assert_access_denied(denied, "signed PutObject after credential revoke").await;
    eprintln!("s3_gateway: signed PutObject after revoke correctly rejected (403)");

    // (4) Re-issue the credential (active again) and engage the global write kill
    // switch; a signed PutObject is denied by the kill switch alone (step 1).
    admin_issue_credential(&admin_base, OPERATOR_TOKEN, &principal_label, &bucket_label).await;
    admin_set_kill_switch(&admin_base, OPERATOR_TOKEN, true).await;
    let denied = signed_put_response(&s3_base, &host, &bucket_label, PUT_KEY, &put_body, PUT_CONTENT_TYPE).await;
    assert_access_denied(denied, "signed PutObject with kill switch engaged").await;
    eprintln!("s3_gateway: signed PutObject with kill switch engaged correctly denied (403)");

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop storage nodes");
}

// =========================================================================
// Admin control-plane API (operator bearer token)
// =========================================================================

/// Send a request to the admin control plane carrying the operator bearer token,
/// optionally with a JSON body. Returns the raw response.
async fn admin_request(
    method: Method,
    url: &str,
    operator_token: &str,
    json_body: Option<String>,
) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build admin client");
    let mut request = client
        .request(method, url)
        .header(AUTHORIZATION, format!("Bearer {operator_token}"));
    if let Some(body) = json_body {
        request = request
            .header(CONTENT_TYPE, "application/json")
            .body(body);
    }
    request.send().await.expect("admin request send")
}

/// Poll the admin control plane (`GET /kill-switch` with the operator token)
/// until it answers `200 OK`, proving the listener is bound and authenticating.
async fn wait_admin_healthy(admin_base: &str, operator_token: &str, timeout: Duration) {
    let url = format!("{admin_base}/kill-switch");
    let start = Instant::now();
    let mut last: Option<StatusCode>;
    loop {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("build admin client");
        match client
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {operator_token}"))
            .send()
            .await
        {
            Ok(response) => {
                if response.status() == StatusCode::OK {
                    return;
                }
                last = Some(response.status());
            }
            Err(_) => last = None,
        }
        if start.elapsed() >= timeout {
            panic!("admin control plane never became healthy within {timeout:?} (last {last:?})");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `POST /credentials` — issue (or re-issue) a credential keyed by the bootstrap
/// access key id, scoped to exactly the bucket, with put+delete caps, acting on
/// behalf of `principal`. Asserts the admin API accepts it (`200`).
async fn admin_issue_credential(
    admin_base: &str,
    operator_token: &str,
    principal: &str,
    bucket: &str,
) {
    let url = format!("{admin_base}/credentials");
    let body = format!(
        r#"{{"access_key_id":"{ACCESS_KEY_ID}","secret_access_key":"{SECRET_ACCESS_KEY}",
        "principal":"{principal}","scope":{{"type":"buckets","buckets":["{bucket}"]}},
        "caps":{{"can_put":true,"can_delete":true,"can_multipart":true}}}}"#
    );
    let response = admin_request(Method::POST, &url, operator_token, Some(body)).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("admin issue-credential should return 200, got {status}: {text}");
    }
}

/// `POST /policy/rules` — add an allow rule admitting `principal` to write
/// `bucket` under the fail-closed default-deny policy. Asserts `200`.
async fn admin_create_policy_rule(
    admin_base: &str,
    operator_token: &str,
    principal: &str,
    bucket: &str,
) {
    let url = format!("{admin_base}/policy/rules");
    let body = format!(
        r#"{{"priority":{POLICY_PRIORITY},"id":{POLICY_ID},"principal":"{principal}",
        "bucket":"{bucket}","action":"any","effect":"allow",
        "reason":"allow test principal to write the test bucket"}}"#
    );
    let response = admin_request(Method::POST, &url, operator_token, Some(body)).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("admin create-policy-rule should return 200, got {status}: {text}");
    }
}

/// `DELETE /credentials/{access_key_id}` — revoke the bootstrap-keyed credential.
/// Asserts `200` (the credential existed).
async fn admin_revoke_credential(admin_base: &str, operator_token: &str) {
    let url = format!("{admin_base}/credentials/{ACCESS_KEY_ID}");
    let response = admin_request(Method::DELETE, &url, operator_token, None).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("admin revoke-credential should return 200, got {status}: {text}");
    }
}

/// `POST /kill-switch` — engage or release the global write kill switch. Asserts
/// `200`.
async fn admin_set_kill_switch(admin_base: &str, operator_token: &str, is_engaged: bool) {
    let url = format!("{admin_base}/kill-switch");
    let body = format!(r#"{{"is_kill_switch_engaged":{is_engaged}}}"#);
    let response = admin_request(Method::POST, &url, operator_token, Some(body)).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("admin set-kill-switch should return 200, got {status}: {text}");
    }
}

// =========================================================================
// SigV4 request signing (mirrors network/gateway/.../s3/sigv4.rs)
// =========================================================================

/// Host authority a `reqwest` client sends in its `Host` header for `s3_base`
/// (`http://127.0.0.1:PORT` -> `127.0.0.1:PORT`); the value the canonical request
/// signs for the `host` signed header.
fn s3_host(s3_base: &str) -> String {
    s3_base
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

/// The signed headers carried on every request, lowercase and already sorted.
const SIGNED_HEADERS: &str = "host;x-amz-content-sha256;x-amz-date";

/// Build the SigV4 auth headers for a request, mirroring the gateway's canonical
/// request assembly exactly. Returns `(authorization, x-amz-date,
/// x-amz-content-sha256)`.
fn sigv4_headers(method: &str, host: &str, path: &str, body: &[u8]) -> (String, String, String) {
    sigv4_headers_with_payload(method, host, path, "", &sha256_hex(body))
}

/// Canonicalize a raw query string the way the gateway does (split, sort by key,
/// `key=value` with bare flags as `key=`). The multipart test params are
/// alphanumeric, so no percent-encoding is needed.
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

/// As [`sigv4_headers`] but with an explicit canonical query string and
/// `x-amz-content-sha256` payload hash (so streamed uploads can sign the
/// `UNSIGNED-PAYLOAD` sentinel and multipart requests can sign their query).
/// Returns `(authorization, x-amz-date, x-amz-content-sha256)`.
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

    // Canonical headers block: one `name:value\n` per signed header, in sorted
    // order. The signed-headers/payload follow the gateway's
    // `"{method}\n{uri}\n{query}\n{headers}\n{signed}\n{payload}"`.
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

/// Format unix seconds as the AWS `YYYYMMDDTHHMMSSZ` basic-UTC timestamp, via the
/// inverse of the gateway's `days_from_civil` (Howard Hinnant's `civil_from_days`).
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

// =========================================================================
// S3 write assertions
// =========================================================================

/// Assert an *unsigned* `PUT /{bucket}/{key}` is rejected `403 AccessDenied`:
/// writes require a SigV4 signature (anonymous reads are public, anonymous writes
/// are not).
async fn assert_s3_unsigned_put_forbidden(base: &str, bucket: &str, key: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}/{key}");
    let response = client
        .put(&url)
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(b"unsigned write should be rejected".to_vec())
        .send()
        .await
        .expect("unsigned put send");
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "unsigned PutObject should be rejected with 403"
    );
    let body = response.text().await.expect("unsigned put body");
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "unsigned write should map to AccessDenied, got: {body}"
    );
}

/// Send a SigV4-signed `PUT /{bucket}/{key}` and return the raw response (so the
/// caller can assert either success or an authorization denial).
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

/// Signed `GET /` (ListBuckets); asserts `200` and that the credential's scoped
/// bucket appears in the `ListAllMyBucketsResult` body.
async fn assert_s3_list_buckets(base: &str, host: &str, bucket: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let (authorization, amz_date, payload_hash) = sigv4_headers("GET", host, "/", b"");
    let response = client
        .get(format!("{base}/"))
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .send()
        .await
        .expect("signed list-buckets send");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::OK,
        "ListBuckets should return 200, got {status}: {body}"
    );
    assert!(
        body.contains(&format!("<Name>{bucket}</Name>")),
        "ListBuckets should list the credential's scoped bucket {bucket}, got: {body}"
    );
}

/// Anonymous `GET /{bucket}` (ListObjects V1); asserts a `200` V1 `ListBucketResult`
/// (carries `<Marker>`, not the V2 `<KeyCount>`) that includes `expected_key`.
async fn assert_s3_list_objects_v1(base: &str, bucket: &str, expected_key: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let response = client
        .get(format!("{base}/{bucket}"))
        .send()
        .await
        .expect("list-objects-v1 send");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::OK,
        "ListObjects V1 should return 200, got {status}: {body}"
    );
    assert!(
        body.contains("<ListBucketResult"),
        "expected a ListBucketResult, got: {body}"
    );
    assert!(
        body.contains("<Marker>"),
        "V1 listing should carry <Marker>, got: {body}"
    );
    assert!(
        !body.contains("<KeyCount>"),
        "V1 listing must not carry the V2 <KeyCount>, got: {body}"
    );
    assert!(
        body.contains(&format!("<Key>{expected_key}</Key>")),
        "ListObjects V1 should include {expected_key}, got: {body}"
    );
}

/// Anonymous `HEAD /{bucket}` (HeadBucket); asserts `200` for an existing bucket.
async fn assert_s3_head_bucket(base: &str, bucket: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let response = client
        .head(format!("{base}/{bucket}"))
        .send()
        .await
        .expect("head bucket send");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "HeadBucket should return 200 for an existing bucket"
    );
}

/// `GET /{bucket}/{key}` with `Range: bytes={start}-{end_inclusive}`; asserts
/// `206 Partial Content`, the requested byte window, and a matching
/// `Content-Range`.
async fn assert_s3_get_range(
    base: &str,
    bucket: &str,
    key: &str,
    full: &[u8],
    start: usize,
    end_inclusive: usize,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let response = client
        .get(format!("{base}/{bucket}/{key}"))
        .header(
            reqwest::header::RANGE,
            format!("bytes={start}-{end_inclusive}"),
        )
        .send()
        .await
        .expect("range get send");
    let status = response.status();
    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.bytes().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::PARTIAL_CONTENT,
        "ranged GET should return 206"
    );
    assert_eq!(
        &body[..],
        &full[start..=end_inclusive],
        "ranged GET should return the requested window"
    );
    assert_eq!(
        content_range.as_deref(),
        Some(format!("bytes {start}-{end_inclusive}/{}", full.len()).as_str()),
        "ranged GET should set Content-Range"
    );
}

/// Anonymous `GET /` (ListBuckets); asserts `403 AccessDenied` (an account
/// operation requires authentication).
async fn assert_s3_list_buckets_anonymous_denied(base: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build s3 client");
    let response = client
        .get(format!("{base}/"))
        .send()
        .await
        .expect("anonymous list-buckets send");
    assert_access_denied(response, "anonymous ListBuckets").await;
}

/// Assert a response is a `403 AccessDenied` S3 XML error.
async fn assert_access_denied(response: reqwest::Response, context: &str) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "{context} should be denied with 403, got {status}: {body}"
    );
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "{context} should map to AccessDenied, got: {body}"
    );
}

/// Assert a SigV4-signed `PUT /{bucket}/{key}` succeeds (`200 OK`) and returns a
/// quoted ETag. Returns the ETag.
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

/// Assert a streamed (`UNSIGNED-PAYLOAD`) SigV4 `PUT /{bucket}/{key}` succeeds
/// (`200 OK`) and returns its quoted ETag.
///
/// The signature is valid but the payload hash is the `UNSIGNED-PAYLOAD` sentinel,
/// so the gateway takes the bounded-memory streamed path (`object_reader` ->
/// `write_object_stream` -> chunk track + manifest) rather than buffering and
/// hash-verifying the body.
async fn assert_s3_streamed_put(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    content_type: &str,
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let url = format!("{base}{path}");
    let (authorization, amz_date, payload_hash) =
        sigv4_headers_with_payload("PUT", host, &path, "", "UNSIGNED-PAYLOAD");

    let response = client
        .put(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .header(CONTENT_TYPE, content_type)
        .body(body.to_vec())
        .send()
        .await
        .expect("streamed put send");
    let status = response.status();
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        panic!("streamed PutObject should return 200, got {status}: {body}");
    }
    etag.expect("streamed PutObject response should include an ETag")
}

/// Extract the text content of the first `<tag>...</tag>` in an XML body.
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// CreateMultipartUpload (`POST /{bucket}/{key}?uploads`); returns the upload id.
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

/// UploadPart (`PUT /{bucket}/{key}?uploadId=..&partNumber=..`); returns the part
/// ETag the gateway minted (echoed back at completion).
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

/// CompleteMultipartUpload (`POST /{bucket}/{key}?uploadId=..`) with the part list
/// XML; returns the object ETag.
async fn s3_complete_multipart(
    base: &str,
    host: &str,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(u32, String)],
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .expect("build s3 client");
    let path = format!("/{bucket}/{key}");
    let raw_query = format!("uploadId={upload_id}");
    let url = format!("{base}{path}?{raw_query}");

    let mut xml = String::from("<CompleteMultipartUpload>");
    for (part_number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");

    let (authorization, amz_date, payload_hash) = sigv4_headers_with_payload(
        "POST",
        host,
        &path,
        &canonical_query(&raw_query),
        &sha256_hex(xml.as_bytes()),
    );

    let response = client
        .post(&url)
        .header("authorization", authorization)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .header(CONTENT_TYPE, "application/xml")
        .body(xml.into_bytes())
        .send()
        .await
        .expect("complete multipart send");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::OK,
        "CompleteMultipartUpload should return 200, got {status}: {body}"
    );
    extract_xml_tag(&body, "ETag").unwrap_or_default()
}

// =========================================================================
// SDK / read helpers (mirror gateway_read.rs)
// =========================================================================

/// Poll the SDK object listing (served by the storage nodes) until `key` appears
/// under `prefix`, proving the named write was ingested and certified network-wide.
async fn wait_sdk_object_listed(
    harness: &SimnetHarness,
    bucket: &Address,
    prefix: &str,
    key: &str,
    timeout: Duration,
) {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());
    let start = Instant::now();
    loop {
        let page = sdk
            .list_objects(bucket, ListObjectsQuery::new(prefix))
            .await
            .expect("sdk list objects");
        if page
            .objects
            .iter()
            .any(|object| object.name.as_slice() == key.as_bytes())
        {
            return;
        }
        if start.elapsed() >= timeout {
            panic!("object {key} never became visible via SDK list within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Poll `HEAD /{bucket}/{key}` until it returns `200 OK`.
async fn wait_s3_head_ok(base: &str, bucket: &str, key: &str, timeout: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}/{key}");
    let start = Instant::now();
    let mut last: Option<StatusCode>;
    loop {
        match client.head(&url).send().await {
            Ok(response) => {
                if response.status() == StatusCode::OK {
                    return;
                }
                last = Some(response.status());
            }
            Err(_) => last = None,
        }
        if start.elapsed() >= timeout {
            panic!(
                "object never became readable via S3 HEAD within {timeout:?} (last status {last:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Assert `GET /{bucket}?list-type=2&prefix=photos/` returns a valid
/// `ListBucketResult` containing exactly the expected key.
async fn assert_s3_list_objects_v2(base: &str, bucket: &str, expected_key: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}?list-type=2&prefix={OBJECT_PREFIX}");
    let response = client.get(&url).send().await.expect("list-objects-v2 send");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "ListObjectsV2 should return 200"
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml"),
        "ListObjectsV2 should be served as XML"
    );
    let body = response.text().await.expect("list-objects-v2 body");

    assert!(
        body.contains("<ListBucketResult"),
        "listing should be a ListBucketResult document, got: {body}"
    );
    assert!(
        body.contains(&format!("<Name>{bucket}</Name>")),
        "listing Name should echo the bucket label, got: {body}"
    );
    assert!(
        body.contains(&format!("<Key>{expected_key}</Key>")),
        "listing should contain the written object key, got: {body}"
    );
    assert!(
        body.contains("<KeyCount>1</KeyCount>"),
        "listing should report a single key, got: {body}"
    );
    assert!(
        body.contains("<IsTruncated>false</IsTruncated>"),
        "single-object listing should not be truncated, got: {body}"
    );
    assert!(
        body.contains("<StorageClass>STANDARD</StorageClass>"),
        "object should report the STANDARD storage class, got: {body}"
    );
}

/// Assert `GET /{bucket}/{key}` returns the exact object bytes plus the expected
/// headers. Returns the (quoted) ETag for cross-checking against HEAD.
async fn assert_s3_get_object(
    base: &str,
    bucket: &str,
    key: &str,
    expected: &[u8],
    expected_content_type: &str,
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}/{key}");
    let response = client.get(&url).send().await.expect("get-object send");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GetObject should return 200"
    );

    let headers = response.headers().clone();
    assert_eq!(
        headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
        Some(expected_content_type),
        "GetObject Content-Type should match the written content type"
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected.len().to_string().as_str()),
        "GetObject Content-Length should match the object size"
    );
    assert_eq!(
        headers
            .get(CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=31536000, immutable"),
        "immutable object should be edge-cacheable"
    );
    let etag = headers
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("GetObject response should include an ETag")
        .to_string();
    assert!(
        etag.starts_with('"') && etag.ends_with('"') && etag.len() > 2,
        "ETag should be quoted and non-empty, got {etag}"
    );

    let bytes = response.bytes().await.expect("get-object body");
    assert_eq!(
        bytes.as_ref(),
        expected,
        "GetObject bytes should match the written object exactly"
    );
    etag
}

/// Assert `HEAD /{bucket}/{key}` returns the right metadata headers and an empty
/// body. Returns the (quoted) ETag for cross-checking against GET.
async fn assert_s3_head_object(
    base: &str,
    bucket: &str,
    key: &str,
    expected_len: usize,
    expected_content_type: &str,
) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}/{key}");
    let response = client.head(&url).send().await.expect("head-object send");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "HeadObject should return 200"
    );

    let headers = response.headers().clone();
    assert_eq!(
        headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
        Some(expected_content_type),
        "HeadObject Content-Type should match the written content type"
    );
    assert_eq!(
        headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected_len.to_string().as_str()),
        "HeadObject Content-Length should match the object size"
    );
    let etag = headers
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("HeadObject response should include an ETag")
        .to_string();
    assert!(
        etag.starts_with('"') && etag.ends_with('"') && etag.len() > 2,
        "ETag should be quoted and non-empty, got {etag}"
    );

    let body = response.bytes().await.expect("head-object body");
    assert!(body.is_empty(), "HeadObject must not carry a body");
    etag
}

/// Assert `GET /{bucket}/{missing}` returns a NoSuchKey S3 XML error (HTTP 404).
async fn assert_s3_no_such_key(base: &str, bucket: &str, missing_key: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build s3 client");
    let url = format!("{base}/{bucket}/{missing_key}");
    let response = client.get(&url).send().await.expect("missing-key send");
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "GET on a missing key should return 404"
    );
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/xml"),
        "S3 error body should be XML"
    );
    let body = response.text().await.expect("missing-key body");
    assert!(
        body.contains("<Code>NoSuchKey</Code>"),
        "missing key should map to the NoSuchKey S3 error code, got: {body}"
    );
    assert!(
        body.contains("<Error>") && body.contains("</Error>"),
        "S3 error should be an <Error> document, got: {body}"
    );
}

/// Wait until every running storage node has discovered the gateway as a peer
/// (by its pinned TLS pubkey). Mirrors the helper in `gateway_read.rs`.
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

/// Poll the gateway's native `/v1/health` endpoint until it reports `200 OK`.
/// Mirrors the helper in `gateway_read.rs`.
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

/// Deterministic pseudo-random bytes, matching `gateway_read.rs`.
fn deterministic_bytes(len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        let mixed = index.wrapping_mul(31) ^ index.rotate_left(5);
        bytes.push(mixed as u8);
    }
    bytes
}
