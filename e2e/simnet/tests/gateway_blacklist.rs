//! Gateways following blacklists written with the SDK: removed tracks and
//! unlisted tapes answer 410 on every read path, local copies are purged, and
//! the denied set follows deletes and expiry, survives restarts, combines
//! across lists and matches on a second gateway.

use std::time::{Duration, Instant};

use tape_peer_http::HttpApi;
use reqwest::header::{HOST, IF_NONE_MATCH, RANGE};
use reqwest::redirect::Policy;
use reqwest::{Client, RequestBuilder, StatusCode};
use rpc_litesvm::LiteSvmRpc;
use tape_api::program::tapedrive::TrackAddress;
use tape_blacklist::batch::{render, render_first};
use tape_blacklist::entry::Entry;
use tape_blacklist::subscription::{HonoredAction, Subscription};
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, TestGateway, run_simnet_test,
};
use tape_rack::format::{render as render_rack, render_first as render_rack_first};
use tape_rack::record::Record;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;
use tape_store::ops::{TrackDataOps, TrackOps};

const NODE_COUNT: usize = TAPENET_GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;
const RESERVE_EPOCHS: u64 = 32;
const EXPIRING_EPOCHS: u64 = 3;
const SUBDOMAIN_SUFFIX: &str = "sites.test";

const REMOVED_BODY: &[u8] = b"content both lists remove";
const KEPT_BODY: &[u8] = b"content no list denies";
const SENTINEL_BODY: &[u8] = b"content only list b removes";
const EARLY_BODY: &[u8] = b"written before the unlisting";
const LATE_BODY: &[u8] = b"written after the unlisting";
const EXPIRING_BODY: &[u8] = b"content an expiring blacklist tape removes";
const INDEX_BODY: &[u8] = b"<html><body>unlisted site</body></html>";

type Writer = Tapedrive<LiteSvmRpc, HttpApi>;

// gateways deny what followed blacklists remove or unlist, across restarts and instances
#[test]
fn followed_blacklists() {
    run_simnet_test(followed_blacklists_inner);
}

async fn followed_blacklists_inner() {
    tape_peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    let mut second =
        TestGateway::new(1, harness.chain().rpc().clone()).expect("build second gateway fixture");
    eprintln!("gateway_blacklist: fixtures built");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let catch_up_timeout = Duration::from_secs(180);
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
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start storage nodes");

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
    eprintln!("gateway_blacklist: storage nodes active at epoch 3");

    let content_key = TapeKey::generate();
    let unlisted_key = TapeKey::generate();
    let rack_a_key = TapeKey::generate();
    let blacklist_a_key = TapeKey::generate();
    let next_blacklist_a_key = TapeKey::generate();
    let rack_b_key = TapeKey::generate();
    let blacklist_b_key = TapeKey::generate();
    let (content, unlisted) = (content_key.address(), unlisted_key.address());

    let writer = scenario.sdk(harness.admin());
    let tapes = [
        &content_key,
        &unlisted_key,
        &rack_a_key,
        &blacklist_a_key,
        &next_blacklist_a_key,
        &rack_b_key,
        &blacklist_b_key,
    ];
    for key in tapes {
        writer
            .reserve(key, StorageUnits::mb(1), RESERVE_EPOCHS)
            .await
            .expect("reserve tape");
    }
    let removed = put_text(&writer, &content_key, "removed.txt", REMOVED_BODY).await;
    let kept = put_text(&writer, &content_key, "kept.txt", KEPT_BODY).await;
    let sentinel = put_text(&writer, &content_key, "sentinel.txt", SENTINEL_BODY).await;
    let early = put_text(&writer, &unlisted_key, "early.txt", EARLY_BODY).await;
    writer
        .put_object(&unlisted_key, "index.html", INDEX_BODY, Some("text/html"))
        .await
        .expect("put site index");

    // Both lists remove the same track before any gateway runs. List a names its
    // blacklist tape after a header-only track 0; list b names it in track 0.
    declare_blacklist(&writer, &blacklist_a_key, &[]).await;
    declare_blacklist(&writer, &blacklist_b_key, &[]).await;
    declare_rack(&writer, &rack_a_key, &[]).await;
    let a_record = add_children(&writer, &rack_a_key, &[&blacklist_a_key]).await;
    declare_rack(&writer, &rack_b_key, &[&blacklist_b_key]).await;
    publish(&writer, &blacklist_a_key, &[Entry::remove(removed)]).await;
    let b_removal = publish(&writer, &blacklist_b_key, &[Entry::remove(removed)]).await;
    eprintln!("gateway_blacklist: content and first decisions written");

    gateway.enable_s3().expect("enable s3 listener");
    gateway.site_config_mut().subdomain_suffix = Some(SUBDOMAIN_SUFFIX.to_string());
    *gateway.blacklists_mut() = vec![
        subscription("a", rack_a_key.address()),
        subscription("b", rack_b_key.address()),
    ];

    for fixture in [&gateway, &second] {
        scenario
            .register_gateway(fixture, BasisPoints(100))
            .await
            .expect("register gateway");
        scenario
            .stake_gateway(fixture, GATEWAY_STAKE)
            .await
            .expect("stake gateway");
        harness
            .wait_gateway_known(fixture, active_timeout)
            .await
            .expect("storage nodes learned gateway peer");
    }
    gateway.start().await.expect("start gateway");
    gateway
        .wait_healthy(catch_up_timeout)
        .await
        .expect("gateway healthy");
    let epoch4 = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 4");
    assert!(epoch4 >= 4, "expected at least epoch 4, got {epoch4}");
    for fixture in [&gateway, &second] {
        scenario
            .advance_gateway_pool_ok(fixture)
            .await
            .expect("advance gateway pool");
    }
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("storage nodes active after gateway activation");
    eprintln!("gateway_blacklist: gateways active");

    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build http client");
    let base = gateway.base_url();
    let s3 = gateway.s3_base_url();
    let object = |track: Address| format!("{base}/object/{track}");

    // Catch-up applies decisions already on chain before any read is served.
    wait_denied(|| client.get(object(removed)), catch_up_timeout).await;
    wait_status(|| client.get(object(kept)), StatusCode::OK, catch_up_timeout).await;
    eprintln!("gateway_blacklist: caught up");

    let content_host = format!("{}.{SUBDOMAIN_SUFFIX}", content.to_subdomain_label());
    let removed_key = format!("{s3}/{content}/removed.txt");
    let removed_reads = [
        client.get(object(removed)),
        client.get(format!("{base}/track/{removed}")),
        client.get(format!("{base}/v1/tracks/{removed}/data")),
        client.get(format!("{base}/v1/tracks/{removed}/slices/0")),
        client.get(format!("{base}/site/{content}/removed.txt")),
        client.get(format!("{base}/removed.txt")).header(HOST, &content_host),
        client.get(&removed_key),
        client.head(&removed_key),
        client.get(&removed_key).header(RANGE, "bytes=0-3"),
        client.get(&removed_key).header(IF_NONE_MATCH, "\"any\""),
    ];
    for request in removed_reads {
        assert_status(&client, request, StatusCode::GONE).await;
    }
    assert_body(&client, client.get(format!("{s3}/{content}/kept.txt")), KEPT_BODY).await;
    assert!(
        !has_data(&gateway, removed),
        "a removed track keeps no data on the gateway"
    );
    eprintln!("gateway_blacklist: removed track gone on every read path");

    // An unlisting in a live block closes the tape, its site and its bucket.
    publish(&writer, &blacklist_a_key, &[Entry::unlist(unlisted)]).await;
    wait_status(
        || client.get(format!("{base}/site/{unlisted}/")),
        StatusCode::GONE,
        active_timeout,
    )
    .await;
    let unlisted_host = format!("{}.{SUBDOMAIN_SUFFIX}", unlisted.to_subdomain_label());
    let unlisted_reads = [
        client.get(object(early)),
        client.get(format!("{base}/")).header(HOST, &unlisted_host),
        client.get(format!("{s3}/{unlisted}/early.txt")),
    ];
    for request in unlisted_reads {
        assert_status(&client, request, StatusCode::GONE).await;
    }
    wait_purged(&gateway, early, active_timeout).await;

    let late = put_text(&writer, &unlisted_key, "late.txt", LATE_BODY).await;
    wait_purged(&gateway, late, active_timeout).await;
    assert_status(&client, client.get(object(late)), StatusCode::GONE).await;
    eprintln!("gateway_blacklist: unlisted tape gone, later track included");

    // List b undoes its removal by deleting the track, and list a's removal stays.
    writer
        .delete(&blacklist_b_key, b_removal)
        .await
        .expect("delete list b's removal");
    publish(&writer, &blacklist_b_key, &[Entry::remove(sentinel)]).await;
    wait_status(|| client.get(object(sentinel)), StatusCode::GONE, active_timeout).await;
    assert_status(&client, client.get(object(removed)), StatusCode::GONE).await;
    eprintln!("gateway_blacklist: lists combine");

    // List a moves its unlisting onto a new blacklist tape and stops naming the old
    // one, so the old tape's removal stops counting and the unlisting stays.
    let next_declaration = declare_blacklist(
        &writer,
        &next_blacklist_a_key,
        &[Entry::unlist(unlisted)],
    )
    .await;
    add_children(&writer, &rack_a_key, &[&next_blacklist_a_key]).await;
    writer
        .delete(&rack_a_key, a_record)
        .await
        .expect("stop naming the old blacklist tape");
    wait_status(|| client.get(object(removed)), StatusCode::OK, active_timeout).await;
    assert_body(&client, client.get(&removed_key), REMOVED_BODY).await;
    assert_status(&client, client.get(object(early)), StatusCode::GONE).await;
    assert_status(&client, client.get(object(late)), StatusCode::GONE).await;
    eprintln!("gateway_blacklist: new blacklist tape followed, unnamed tape dropped");

    // A blacklist tape stops counting once it expires.
    let expiring_key = TapeKey::generate();
    let expiring = put_text(&writer, &content_key, "expiring.txt", EXPIRING_BODY).await;
    let expiring_tape = writer
        .reserve(&expiring_key, StorageUnits::mb(1), EXPIRING_EPOCHS)
        .await
        .expect("reserve expiring tape");
    declare_blacklist(&writer, &expiring_key, &[Entry::remove(expiring)]).await;
    add_children(&writer, &rack_a_key, &[&expiring_key]).await;
    wait_status(|| client.get(object(expiring)), StatusCode::GONE, active_timeout).await;
    while scenario.current_epoch_number().await.expect("read epoch")
        < expiring_tape.expiry_epoch.as_u64()
    {
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance epoch");
    }
    wait_status(|| client.get(object(expiring)), StatusCode::OK, active_timeout).await;
    assert_status(&client, client.get(object(early)), StatusCode::GONE).await;
    eprintln!("gateway_blacklist: expired blacklist tape stops counting");

    // A restart with a blacklist track's bytes gone fetches them from peers and
    // denies the same set.
    gateway
        .context()
        .store
        .delete_track_data(next_declaration)
        .expect("drop a blacklist track's bytes");
    gateway.stop().await.expect("stop gateway");
    gateway.start().await.expect("restart gateway");
    wait_denied(|| client.get(object(early)), catch_up_timeout).await;
    wait_denied(|| client.get(object(sentinel)), catch_up_timeout).await;
    assert_status(&client, client.get(object(late)), StatusCode::GONE).await;
    wait_status(|| client.get(object(removed)), StatusCode::OK, active_timeout).await;
    wait_status(|| client.get(object(expiring)), StatusCode::OK, active_timeout).await;
    assert!(
        has_data(&gateway, next_declaration),
        "catch-up stores the bytes it fetched"
    );
    eprintln!("gateway_blacklist: denials survive a restart");

    // Dropping list b drops its removal and nothing else.
    gateway.stop().await.expect("stop gateway");
    gateway
        .blacklists_mut()
        .retain(|subscription| subscription.name != "b");
    gateway.start().await.expect("restart gateway without list b");
    wait_status(|| client.get(object(sentinel)), StatusCode::OK, catch_up_timeout).await;
    assert_status(&client, client.get(object(early)), StatusCode::GONE).await;
    eprintln!("gateway_blacklist: dropped subscription contributes nothing");

    // A gateway starting from an empty store never serves denied content while it catches up.
    *second.blacklists_mut() = gateway.blacklists_mut().clone();
    second.start().await.expect("start second gateway");
    let second_base = second.base_url();
    wait_denied(
        || client.get(format!("{second_base}/object/{early}")),
        catch_up_timeout,
    )
    .await;
    wait_status(
        || client.get(format!("{second_base}/object/{kept}")),
        StatusCode::OK,
        catch_up_timeout,
    )
    .await;
    for track in [removed, kept, sentinel, early, late, expiring] {
        let first = status(&client, client.get(object(track))).await;
        let other = status(&client, client.get(format!("{second_base}/object/{track}"))).await;
        assert_eq!(first, other, "gateways disagree on {track}");
    }
    eprintln!("gateway_blacklist: second gateway denies the same set");

    second.stop().await.expect("stop second gateway");
    gateway.stop().await.expect("stop gateway");
}

fn subscription(name: &str, tape: Address) -> Subscription {
    Subscription {
        name: name.to_string(),
        tape,
        actions: vec![HonoredAction::Remove, HonoredAction::Unlist],
    }
}

async fn put_text(writer: &Writer, tape: &TapeKey, name: &str, body: &[u8]) -> Address {
    writer
        .put_object(tape, name, body, Some("text/plain"))
        .await
        .expect("put object")
}

/// Write a rack's track 0: the header and any child tapes
async fn declare_rack(writer: &Writer, rack: &TapeKey, children: &[&TapeKey]) -> Address {
    write_inline(writer, rack, &render_rack_first(&records(children))).await
}

/// Write a rack track naming child tapes
async fn add_children(writer: &Writer, rack: &TapeKey, children: &[&TapeKey]) -> Address {
    write_inline(writer, rack, &render_rack(&records(children))).await
}

/// Write a blacklist tape's track 0: the header and any entries
async fn declare_blacklist(writer: &Writer, list: &TapeKey, entries: &[Entry]) -> Address {
    let bytes = render_first(entries).expect("render declaration");
    write_inline(writer, list, &bytes).await
}

/// Write entries on a blacklist tape as one track
async fn publish(writer: &Writer, list: &TapeKey, entries: &[Entry]) -> Address {
    let bytes = render(entries).expect("render entries");
    write_inline(writer, list, &bytes).await
}

async fn write_inline(writer: &Writer, tape: &TapeKey, bytes: &[u8]) -> Address {
    let track = writer
        .write_track(tape, bytes)
        .await
        .expect("write track");
    assert!(track.is_inline(), "rack and blacklist tracks are inline");
    track.address()
}

fn records(children: &[&TapeKey]) -> Vec<Record> {
    children
        .iter()
        .map(|child| Record::new(child.address()))
        .collect()
}

fn has_data(gateway: &TestGateway, track: Address) -> bool {
    gateway
        .context()
        .store
        .has_track_data(track)
        .expect("read gateway track data")
}

/// Wait until the gateway knows a track and holds none of its data
async fn wait_purged(gateway: &TestGateway, track: Address, timeout: Duration) {
    let start = Instant::now();
    loop {
        let store = gateway.context().store.clone();
        let is_known = store.get_track(track).expect("read gateway track").is_some();
        if is_known && !has_data(gateway, track) {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "track {track} still holds data on the gateway"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn status(client: &Client, request: RequestBuilder) -> StatusCode {
    let request = request.build().expect("build request");
    client
        .execute(request)
        .await
        .expect("send request")
        .status()
}

async fn assert_status(client: &Client, request: RequestBuilder, expected: StatusCode) {
    let request = request.build().expect("build request");
    let described = format!("{} {}", request.method(), request.url());
    let response = client.execute(request).await.expect("send request");
    assert_eq!(response.status(), expected, "{described}");
}

async fn assert_body(client: &Client, request: RequestBuilder, expected: &[u8]) {
    let request = request.build().expect("build request");
    let described = format!("{} {}", request.method(), request.url());
    let response = client.execute(request).await.expect("send request");
    assert_eq!(response.status(), StatusCode::OK, "{described}");
    let body = response.bytes().await.expect("read body");
    assert_eq!(body.as_ref(), expected, "{described}");
}

/// Poll until a request answers the expected status
async fn wait_status(request: impl Fn() -> RequestBuilder, expected: StatusCode, timeout: Duration) {
    let start = Instant::now();
    let mut last = None;
    loop {
        if let Ok(response) = request().send().await {
            if response.status() == expected {
                return;
            }
            last = Some(response.status());
        }
        assert!(
            start.elapsed() < timeout,
            "timed out waiting for {expected}, last answer {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Poll until a denied read answers 410, failing if it is ever served meanwhile
async fn wait_denied(request: impl Fn() -> RequestBuilder, timeout: Duration) {
    let start = Instant::now();
    let mut last = None;
    loop {
        if let Ok(response) = request().send().await {
            match response.status() {
                StatusCode::GONE => return,
                StatusCode::OK => panic!("denied content served while catching up"),
                other => last = Some(other),
            }
        }
        assert!(
            start.elapsed() < timeout,
            "timed out waiting for 410, last answer {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
