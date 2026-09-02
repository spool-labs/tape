//! A live S3 gateway over simnet, held up until the process is killed
//!
//! Boots the same network `s3_gateway.rs` boots, issues one credential for one
//! bucket, prints the connection details, and then keeps the chain running so an
//! off-the-shelf S3 client can be pointed at it. Run with:
//!
//! `cargo test -p tape-e2e-simnet --release --test s3_playground -- --ignored --nocapture`
//!
//! `S3_PLAYGROUND_ENV` names a file the details are also written to, in `KEY=value` lines.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Method, StatusCode};
use solana_signer::Signer;

use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TestGateway, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;

/// Epochs the bucket stays reserved, half the capacity schedule horizon
const RESERVE_EPOCHS: u64 = 128;

/// Bytes the bucket can hold, sized for the whole tool matrix
const RESERVE_MB: u64 = 1_024;

/// Port the S3 listener takes, reachable from a container at host.docker.internal
const S3_PORT: u16 = 9000;

const ACCESS_KEY_ID: &str = "TAPEPLAYGROUNDKEY";
const SECRET_ACCESS_KEY: &str = "tape-playground-secret-not-for-production";
const OPERATOR_TOKEN: &str = "s3-operator-secret-token";
const SERVER_PEPPER: &str = "s3-server-pepper";
const POLICY_PRIORITY: u32 = 10;
const POLICY_ID: u64 = 1;

#[test]
#[ignore = "a live playground, not an assertion"]
fn playground() {
    run_simnet_test(playground_inner);
}

async fn playground_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    let (_, admin_addr) = gateway
        .enable_s3_admin_writes(ACCESS_KEY_ID, SECRET_ACCESS_KEY, OPERATOR_TOKEN, SERVER_PEPPER)
        .expect("enable s3 admin write path");
    let s3_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, S3_PORT));
    gateway.set_s3_listen(s3_addr);
    eprintln!("playground: s3 listener on {s3_addr}, admin on {admin_addr}");

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
        eprintln!("playground: network started");
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
        for expected in [2, 3] {
            let epoch = scenario
                .self_advance_epoch(epoch_timeout)
                .await
                .expect("advance epoch");
            assert_eq!(epoch, expected, "expected epoch {expected}");
        }
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active at epoch 3");
        eprintln!("playground: storage nodes active at epoch 3");
    }

    let tape_key = TapeKey::generate();
    let bucket = tape_key.address();
    let gateway_delegate = Address::from(gateway.authority());
    let principal = Address::from(harness.admin().pubkey());
    {
        let scenario = harness.scenario();
        let writer = scenario.sdk(harness.admin());
        writer
            .reserve(&tape_key, StorageUnits::mb(RESERVE_MB), RESERVE_EPOCHS)
            .await
            .expect("reserve bucket tape");
        writer
            .set_tape_delegate(&tape_key, gateway_delegate)
            .await
            .expect("delegate bucket writes to the gateway keypair");
        eprintln!("playground: bucket {bucket} reserved and delegated to {gateway_delegate}");
    }

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
        harness
            .wait_gateway_known(&gateway, active_timeout)
            .await
            .expect("storage nodes learned gateway peer");

        gateway.start().await.expect("start gateway");
        gateway
            .wait_healthy(Duration::from_secs(180))
            .await
            .expect("gateway healthy");

        let epoch4 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 4");
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
        eprintln!("playground: gateway pool advanced, nodes active");
    }

    let admin_base = gateway.s3_admin_base_url();
    let bucket_label = bucket.to_string();
    let label = bucket.to_subdomain_label();
    let principal_label = principal.to_string();
    admin_issue_credential(&admin_base, &principal_label, &bucket_label).await;
    admin_create_policy_rule(&admin_base, &principal_label, &bucket_label).await;

    let endpoint = format!("http://127.0.0.1:{S3_PORT}");
    let probe = reqwest::get(&endpoint).await.expect("probe the s3 listener");
    eprintln!("playground: anonymous GET / answered {}", probe.status());

    let details = format!(
        "S3_ENDPOINT={endpoint}\n\
         S3_DOCKER_ENDPOINT=http://host.docker.internal:{S3_PORT}\n\
         S3_BUCKET={bucket_label}\n\
         S3_BUCKET_LABEL={label}\n\
         AWS_ACCESS_KEY_ID={ACCESS_KEY_ID}\n\
         AWS_SECRET_ACCESS_KEY={SECRET_ACCESS_KEY}\n\
         S3_ADMIN={admin_base}\n\
         S3_OPERATOR_TOKEN={OPERATOR_TOKEN}\n"
    );
    eprintln!("playground: ready\n{details}");
    if let Ok(path) = std::env::var("S3_PLAYGROUND_ENV") {
        std::fs::write(&path, &details).expect("write the playground env file");
        eprintln!("playground: details written to {path}");
    }

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let scenario = harness.scenario();
        let epoch = scenario.current_epoch_number().await.unwrap_or(0);
        let active = scenario
            .wait_nodes_active(&all, Duration::from_secs(1))
            .await
            .is_ok();
        eprintln!("playground: epoch {epoch}, all nodes active: {active}");
    }
}

async fn admin_request(method: Method, url: &str, json_body: Option<String>) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build admin client");
    let mut request = client
        .request(method, url)
        .header(AUTHORIZATION, format!("Bearer {OPERATOR_TOKEN}"));
    if let Some(body) = json_body {
        request = request.header(CONTENT_TYPE, "application/json").body(body);
    }
    request.send().await.expect("admin request")
}

async fn admin_issue_credential(admin_base: &str, principal: &str, bucket: &str) {
    let url = format!("{admin_base}/credentials");
    let body = format!(
        r#"{{"access_key_id":"{ACCESS_KEY_ID}","secret_access_key":"{SECRET_ACCESS_KEY}",
        "principal":"{principal}","scope":{{"type":"buckets","buckets":["{bucket}"]}},
        "caps":{{"can_put":true,"can_delete":true,"can_multipart":true}}}}"#
    );
    let response = admin_request(Method::POST, &url, Some(body)).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("issue credential returned {status}: {text}");
    }
}

async fn admin_create_policy_rule(admin_base: &str, principal: &str, bucket: &str) {
    let url = format!("{admin_base}/policy/rules");
    let body = format!(
        r#"{{"priority":{POLICY_PRIORITY},"id":{POLICY_ID},"principal":"{principal}",
        "bucket":"{bucket}","action":"any","effect":"allow",
        "reason":"the playground principal writes the playground bucket"}}"#
    );
    let response = admin_request(Method::POST, &url, Some(body)).await;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        panic!("create policy rule returned {status}: {text}");
    }
}
