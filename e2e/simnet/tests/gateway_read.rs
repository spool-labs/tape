use std::time::{Duration, Instant};

use reqwest::StatusCode;
use tape_api::program::tapedrive::track_pda;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::GROUP_SIZE;
use tape_core::types::{BasisPoints, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetScenario, TestGateway, run_simnet_test,
};
use tape_protocol::api::{NodeStats, slice_url};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 5;
const GATEWAY_STAKE: u64 = 2_000;
const STORAGE_NODE_STAKE: u64 = 1_000;
const ACCESS_THRESHOLD: u64 = 1;

#[test]
fn staked_gateway() {
    run_simnet_test(staked_gateway_inner);
}

async fn staked_gateway_inner() {
    peer_tls::install_default_provider();

    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");
    let mut gateway =
        TestGateway::new(0, harness.chain().rpc().clone()).expect("build gateway fixture");
    eprintln!("gateway_read: fixtures built");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let slice_timeout = Duration::from_secs(120);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        eprintln!("gateway_read: system initialized");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register storage nodes");
        eprintln!("gateway_read: storage nodes registered");
        scenario
            .stake_all(STORAGE_NODE_STAKE)
            .await
            .expect("stake storage nodes");
        eprintln!("gateway_read: storage nodes staked");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        eprintln!("gateway_read: storage node spool groups set");
        scenario.start_network().await.expect("start network");
        eprintln!("gateway_read: network started");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start storage nodes");
    eprintln!("gateway_read: storage runtimes started");

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .expect("storage nodes healthy");
        eprintln!("gateway_read: storage nodes healthy");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active");
        eprintln!("gateway_read: storage nodes active");

        let epoch2 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 2");
        assert_eq!(epoch2, 2, "expected epoch 2");
        eprintln!("gateway_read: advanced to epoch 2");

        let epoch3 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 3");
        assert_eq!(epoch3, 3, "expected epoch 3");
        eprintln!("gateway_read: advanced to epoch 3");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active at epoch 3");
        eprintln!("gateway_read: storage nodes active at epoch 3");
        assert_group_counts(&scenario, TARGET_GROUPS, TARGET_GROUPS).await;
    }

    let scenario = harness.scenario();
    let data = deterministic_bytes(64 * 1024);
    let reserve_capacity = StorageUnits::from_bytes(data.len() as u64) + StorageUnits::mb(2);
    let writer = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    writer
        .reserve(&tape_key, reserve_capacity, 4)
        .await
        .expect("reserve tape");
    eprintln!("gateway_read: tape reserved");
    let track = writer
        .write_track(&tape_key, &data)
        .await
        .expect("write coded track before gating");
    eprintln!("gateway_read: coded track written");
    assert!(track.is_coded(), "test track must use coded slice reads");
    let track_address = track_pda(track.tape, track.track_number).0;
    wait_current_owner_slices(
        &scenario,
        &track_address,
        track.group,
        GROUP_SIZE,
        slice_timeout,
    )
    .await
    .expect("all current owners should have their slice before gating");
    eprintln!("gateway_read: all owner slices available before gating");

    {
        let scenario = harness.scenario();
        scenario
            .register_gateway(&gateway, BasisPoints(100))
            .await
            .expect("register gateway");
        eprintln!("gateway_read: gateway registered");
        scenario
            .stake_gateway(&gateway, GATEWAY_STAKE)
            .await
            .expect("stake gateway");
        eprintln!("gateway_read: gateway staked");
        wait_gateway_known_by_storage_nodes(&harness, &gateway, active_timeout)
            .await
            .expect("storage nodes learned gateway peer");
        eprintln!("gateway_read: storage nodes learned gateway peer");

        gateway.start().await.expect("start gateway before gating");
        eprintln!("gateway_read: gateway runtime started before gating");
        wait_gateway_healthy(&gateway.base_url(), Duration::from_secs(180))
            .await
            .expect("gateway healthy before gating");
        eprintln!("gateway_read: gateway healthy before gating");

        let epoch4 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 4");
        assert_eq!(epoch4, 4, "expected epoch 4");
        eprintln!("gateway_read: advanced to epoch 4");

        let epoch5 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 5");
        assert_eq!(epoch5, 5, "expected epoch 5");
        eprintln!("gateway_read: advanced to epoch 5");

        scenario
            .advance_gateway_pool_ok(&gateway)
            .await
            .expect("advance gateway pool");
        eprintln!("gateway_read: gateway pool advanced");

        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("storage nodes active after gateway stake activation");
        eprintln!("gateway_read: storage nodes active after gateway stake activation");
    }

    harness.stop_all().await.expect("stop storage nodes");
    eprintln!("gateway_read: storage nodes stopped for threshold update");

    {
        let scenario = harness.scenario();
        scenario
            .set_access_threshold_many(&all, ACCESS_THRESHOLD)
            .await
            .expect("set storage-node access thresholds");
        eprintln!("gateway_read: access thresholds set");
    }

    harness
        .start_nodes_with_retry(&all, 3, Duration::from_millis(200))
        .await
        .expect("restart gated storage nodes");
    eprintln!("gateway_read: gated storage nodes restarted");

    {
        let scenario = harness.scenario();
        scenario
            .wait_nodes_healthy(health_timeout)
            .await
            .expect("restarted storage nodes healthy");
        eprintln!("gateway_read: restarted storage nodes healthy");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("restarted storage nodes active");
        eprintln!("gateway_read: restarted storage nodes active");
    }

    assert_direct_slice_forbidden(&harness, &track_address, track.group)
        .await
        .expect("anonymous storage-node slice read should be forbidden");
    eprintln!("gateway_read: direct storage-node read forbidden");

    let stats_before = gateway_stats(&gateway.base_url())
        .await
        .expect("gateway stats before read");
    assert_eq!(
        stats_before.slices_stored, 0,
        "gateway cache should start cold"
    );

    let gateway_reader =
        Tapedrive::new_gateway_read_only(harness.chain().rpc().clone(), gateway.base_url())
            .expect("gateway read-only SDK");
    let gateway_read = gateway_reader
        .read(&track_address)
        .await
        .expect("read through gateway");
    eprintln!("gateway_read: gateway SDK read succeeded");
    assert_eq!(gateway_read, data, "gateway read should reconstruct data");

    assert_gateway_decoded_route(&gateway.base_url(), "object", &track_address, &data)
        .await
        .expect("gateway object endpoint should return decoded bytes");
    eprintln!("gateway_read: gateway object endpoint succeeded");
    assert_gateway_decoded_route(&gateway.base_url(), "track", &track_address, &data)
        .await
        .expect("gateway track endpoint should return decoded bytes");
    eprintln!("gateway_read: gateway track endpoint succeeded");

    let stats_after = gateway_stats(&gateway.base_url())
        .await
        .expect("gateway stats after read");
    assert!(
        stats_after.slices_stored > 0,
        "gateway read should cache at least one slice"
    );
    assert!(
        stats_after.slices_stored >= u64::from(EncodingProfile::clay_default().k()),
        "gateway should cache enough slices for offline decode"
    );

    harness.stop_all().await.expect("stop gated storage nodes");
    eprintln!("gateway_read: gated storage nodes stopped");

    let cached_read = gateway_reader
        .read(&track_address)
        .await
        .expect("gateway cached read with storage nodes offline");
    assert_eq!(cached_read, data, "cached gateway read should match data");
    eprintln!("gateway_read: cached gateway read succeeded");

    assert_gateway_decoded_route(&gateway.base_url(), "object", &track_address, &data)
        .await
        .expect("gateway cached object endpoint with storage nodes offline");
    eprintln!("gateway_read: cached gateway object endpoint succeeded");
    assert_gateway_decoded_route(&gateway.base_url(), "track", &track_address, &data)
        .await
        .expect("gateway cached track endpoint with storage nodes offline");
    eprintln!("gateway_read: cached gateway track endpoint succeeded");

    gateway.stop().await.expect("stop gateway");
}

async fn assert_gateway_decoded_route(
    gateway_base: &str,
    route: &str,
    track: &Address,
    expected: &[u8],
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let response = client
        .get(format!("{gateway_base}/{route}/{track}"))
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "gateway {route} endpoint should return 200"
    );

    let headers = response.headers().clone();
    assert_eq!(
        headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/octet-stream"),
        "unnamed track should use fallback content type"
    );
    let expected_content_length = expected.len().to_string();
    assert_eq!(
        headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(expected_content_length.as_str()),
        "object content length should match decoded bytes"
    );
    let etag = headers
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("object response should include etag");
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "etag should be quoted"
    );
    assert_eq!(
        headers
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=31536000, immutable"),
        "immutable track should be edge-cacheable"
    );

    let bytes = response.bytes().await?;
    assert_eq!(bytes.as_ref(), expected, "object bytes should match track data");
    Ok(())
}

async fn assert_direct_slice_forbidden(
    harness: &tape_e2e_simnet::SimnetHarness,
    track: &Address,
    group: tape_core::spooler::GroupIndex,
) -> anyhow::Result<()> {
    let scenario = harness.scenario();
    let system = scenario.read_system().await?;
    let group_account = scenario.read_group(system.current_epoch, group).await?;
    let (position, owner) = group_account
        .spools
        .iter()
        .enumerate()
        .find_map(|(position, spool)| {
            (spool.node != Address::default()).then_some((position, spool.node))
        })
        .expect("group has at least one assigned spool");
    let owner_node = harness
        .nodes()
        .iter()
        .find(|node| {
            node.is_running()
                && Address::from(scenario.node_address(node.id())) == owner
        })
        .expect("assigned owner node is running");
    let spool = group.spool_at(position);

    let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
    let builder = peer_tls::apply_pinned_tls(builder, owner_node.tls_pubkey())
        .expect("anonymous pinned tls");
    let client = builder.build().expect("anonymous client build");
    let response = client
        .get(format!(
            "{}{}",
            owner_node.base_url(),
            slice_url(&track.to_string(), spool)
        ))
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "anonymous storage-node read should be blocked by StakedPeer"
    );
    Ok(())
}

async fn wait_gateway_known_by_storage_nodes(
    harness: &tape_e2e_simnet::SimnetHarness,
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

async fn wait_current_owner_slices(
    scenario: &SimnetScenario<'_>,
    track: &Address,
    group: tape_core::spooler::GroupIndex,
    expected: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();

    loop {
        let observed = scenario.count_current_owner_slices(track, group).await?;
        if observed == expected {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            anyhow::bail!(
                "timed out waiting for current group owners to hold {expected} slices, observed {observed}"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

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

async fn gateway_stats(base: &str) -> anyhow::Result<NodeStats> {
    Ok(reqwest::Client::new()
        .get(format!("{base}/v1/stats"))
        .send()
        .await?
        .error_for_status()?
        .json::<NodeStats>()
        .await?)
}

async fn assert_group_counts(
    scenario: &SimnetScenario<'_>,
    expected_target: u64,
    expected_live: u64,
) {
    let system = scenario.read_system().await.expect("read system");

    assert_eq!(
        system.target_group_count, expected_target,
        "unexpected target group count"
    );
    assert_eq!(
        system.live_group_count, expected_live,
        "unexpected live group count"
    );
}

fn deterministic_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            let mixed = i.wrapping_mul(31) ^ i.rotate_left(5);
            mixed as u8
        })
        .collect()
}
