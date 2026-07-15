//! Site-route serving over the gateway's native listener: a small site is
//! written as named objects, then served by path with inline rendering,
//! index and 404 defaults, revalidation, and the download escape hatch.

use std::time::{Duration, Instant};

use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TestGateway, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 8;

const INDEX_BODY: &[u8] = b"<html><body>site index</body></html>";
const STYLE_BODY: &[u8] = b"body { color: #01a2f2; }";
const MISSING_BODY: &[u8] = b"<html><body>lost tape</body></html>";

const SITE_POLICY: &[u8] =
    br#"{"max_age_secs": 5, "connect_origins": ["https://rpc.test"]}"#;

const CUSTOM_DOMAIN: &str = "mysite.test";
const SUBDOMAIN_SUFFIX: &str = "sites.test";
const ALLOWED_ORIGIN: &str = "https://app.example";

// the site route serves a named-object site by path with web semantics
#[test]
fn site_serving() {
    run_simnet_test(site_serving_inner);
}

async fn site_serving_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    eprintln!("gateway_site: fixtures built");

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
        eprintln!("gateway_site: network started");
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
        eprintln!("gateway_site: storage nodes active at epoch 3");
    }

    // The site: an index page, a typed 404 page, and a stylesheet stored with
    // no content type so serving must infer text/css from the extension.
    let tape_key = TapeKey::generate();
    let tape = tape_key.address();
    {
        let scenario = harness.scenario();
        let writer = scenario.sdk(harness.admin());
        writer
            .reserve(&tape_key, StorageUnits::mb(1), RESERVE_EPOCHS)
            .await
            .expect("reserve site tape");
        writer
            .put_object(&tape_key, "index.html", INDEX_BODY, Some("text/html"))
            .await
            .expect("put index page");
        writer
            .put_object(&tape_key, "assets/app.css", STYLE_BODY, None)
            .await
            .expect("put stylesheet");
        writer
            .put_object(&tape_key, "404.html", MISSING_BODY, Some("text/html"))
            .await
            .expect("put 404 page");
        writer
            .put_object(&tape_key, "_site.json", SITE_POLICY, Some("application/json"))
            .await
            .expect("put site policy");
        eprintln!("gateway_site: site objects written");
    }

    // Host-based serving: one custom domain, the subdomain suffix, and a
    // single allowed cross-origin reader.
    {
        let site = gateway.site_config_mut();
        site.domains.insert(CUSTOM_DOMAIN.to_string(), tape);
        site.subdomain_suffix = Some(SUBDOMAIN_SUFFIX.to_string());
        site.cors_origins = vec![ALLOWED_ORIGIN.to_string()];
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
        eprintln!("gateway_site: gateway healthy");

        let epoch4 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 4");
        assert!(epoch4 >= 4, "expected at least epoch 4, got {epoch4}");
        scenario
            .advance_gateway_pool_ok(&gateway)
            .await
            .expect("advance gateway pool");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active after gateway activation");
        eprintln!("gateway_site: gateway active");
    }

    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build http client");
    let site_base = format!("{}/site/{tape}", gateway.base_url());

    // The mirror needs a beat to ingest the site writes into its object index.
    wait_site_ready(&client, &site_base, active_timeout)
        .await
        .expect("site served through the gateway");

    // Bare site URL redirects to the slash form.
    let response = client
        .get(&site_base)
        .send()
        .await
        .expect("request site root");
    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        header(&response, "location"),
        format!("/site/{tape}/"),
        "root should redirect to the slash form"
    );
    eprintln!("gateway_site: root redirect ok");

    // The slash form serves the index page inline with site semantics.
    let response = client
        .get(format!("{site_base}/"))
        .send()
        .await
        .expect("request site index");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), "text/html");
    assert_eq!(
        header(&response, "cache-control"),
        "public, max-age=5, must-revalidate",
        "tenant policy should shorten the revalidation window"
    );
    assert!(
        header(&response, "content-security-policy")
            .ends_with("connect-src 'self' https://rpc.test"),
        "tenant policy should extend connect-src"
    );
    assert_eq!(header(&response, "x-content-type-options"), "nosniff");
    assert!(
        response
            .headers()
            .get("content-security-policy")
            .is_some(),
        "site responses carry a content security policy"
    );
    assert!(
        response.headers().get("content-disposition").is_none(),
        "site responses render inline"
    );
    let etag = header(&response, "etag");
    let body = response.bytes().await.expect("read index body");
    assert_eq!(body.as_ref(), INDEX_BODY);
    eprintln!("gateway_site: index served inline");

    // A matching If-None-Match revalidates without a body.
    let response = client
        .get(format!("{site_base}/"))
        .header("if-none-match", &etag)
        .send()
        .await
        .expect("revalidate site index");
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    eprintln!("gateway_site: revalidation ok");

    // An untyped stylesheet serves with the type inferred from its extension.
    let response = client
        .get(format!("{site_base}/assets/app.css"))
        .send()
        .await
        .expect("request stylesheet");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), "text/css");
    let body = response.bytes().await.expect("read stylesheet body");
    assert_eq!(body.as_ref(), STYLE_BODY);
    eprintln!("gateway_site: stylesheet inferred and served");

    // A miss serves the site's 404 page with a 404 status.
    let response = client
        .get(format!("{site_base}/no/such/page"))
        .send()
        .await
        .expect("request missing page");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response.bytes().await.expect("read 404 body");
    assert_eq!(body.as_ref(), MISSING_BODY);
    eprintln!("gateway_site: 404 page served");

    // The download query flips back to attachment behavior.
    let response = client
        .get(format!("{site_base}/index.html?download=1"))
        .send()
        .await
        .expect("request download");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        header(&response, "content-disposition"),
        "attachment; filename*=UTF-8''index.html"
    );
    eprintln!("gateway_site: download query ok");

    // A custom domain serves the site from the domain root.
    let base = gateway.base_url();
    let response = client
        .get(format!("{base}/"))
        .header("host", CUSTOM_DOMAIN)
        .send()
        .await
        .expect("request custom domain root");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), "text/html");
    assert_eq!(header(&response, "x-content-type-options"), "nosniff");
    let body = response.bytes().await.expect("read domain index");
    assert_eq!(body.as_ref(), INDEX_BODY);
    eprintln!("gateway_site: custom domain root served");

    let response = client
        .get(format!("{base}/assets/app.css"))
        .header("host", CUSTOM_DOMAIN)
        .send()
        .await
        .expect("request custom domain asset");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), "text/css");
    eprintln!("gateway_site: custom domain asset served");

    // The tape's subdomain label serves the same site under the suffix.
    let label = tape.to_subdomain_label();
    let response = client
        .get(format!("{base}/"))
        .header("host", format!("{label}.{SUBDOMAIN_SUFFIX}"))
        .send()
        .await
        .expect("request subdomain root");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.bytes().await.expect("read subdomain index");
    assert_eq!(body.as_ref(), INDEX_BODY);
    eprintln!("gateway_site: subdomain root served");

    // The allowed origin earns the cross-origin header, others get nothing.
    let response = client
        .get(format!("{base}/"))
        .header("host", CUSTOM_DOMAIN)
        .header("origin", ALLOWED_ORIGIN)
        .send()
        .await
        .expect("request with allowed origin");
    assert_eq!(header(&response, "access-control-allow-origin"), ALLOWED_ORIGIN);
    let response = client
        .get(format!("{base}/"))
        .header("host", CUSTOM_DOMAIN)
        .header("origin", "https://other.example")
        .send()
        .await
        .expect("request with unlisted origin");
    assert!(response.headers().get("access-control-allow-origin").is_none());
    eprintln!("gateway_site: cors headers ok");

    // An unmapped host still reaches the normal routes.
    let response = client
        .get(format!("{base}/v1/health"))
        .header("host", "unmapped.test")
        .send()
        .await
        .expect("request health on unmapped host");
    assert_eq!(response.status(), StatusCode::OK);
    eprintln!("gateway_site: unmapped host falls through");

    gateway.stop().await.expect("stop gateway");
    harness.stop_all().await.expect("stop storage nodes");
    eprintln!("gateway_site: complete");
}

fn header(response: &reqwest::Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Poll the site index until the gateway's mirror serves it.
async fn wait_site_ready(client: &Client, site_base: &str, timeout: Duration) -> anyhow::Result<()> {
    let start = Instant::now();
    loop {
        if let Ok(response) = client.get(format!("{site_base}/")).send().await {
            if response.status() == StatusCode::OK {
                return Ok(());
            }
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("timed out waiting for the site index to serve");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

