//! Basic end-to-end flow tests.
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test basic_flow -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    TestContext, wait_for_node_health, temp_file_with_content, random_blob, sizes,
};

/// Basic single-node test.
///
/// This test:
/// 1. Spins up a local validator
/// 2. Initializes the system
/// 3. Registers and starts a single node
/// 4. Uploads a file
/// 5. Downloads and verifies the file
#[tokio::test]
#[ignore]
#[serial]
async fn test_basic_upload_download() {
    println!("=== Basic Upload/Download Test ===");

    // Setup with one node
    let ctx = TestContext::builder()
        .nodes(1)
        .port(8080)
        .timeout(Duration::from_secs(120))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Wait for node to be healthy
    wait_for_node_health(&ctx.nodes[0].url(), Duration::from_secs(30))
        .await
        .expect("Node did not become healthy");

    println!("Node healthy at {}", ctx.nodes[0].url());

    // Create test data
    let blob = random_blob(sizes::SMALL);
    let upload_file = temp_file_with_content(&blob)
        .expect("Failed to create temp file");

    // Upload
    let upload_result = ctx.cli.storage_upload(
        upload_file.path(),
        None, // auto-create tape
        Some(&[ctx.nodes[0].url()]),
    ).expect("Failed to upload");

    println!("Uploaded track: {}", upload_result.track_id);

    // Download
    let download_file = tempfile::NamedTempFile::new()
        .expect("Failed to create download file");

    ctx.cli.storage_download(
        &upload_result.track_id,
        download_file.path(),
        Some(&[ctx.nodes[0].url()]),
    ).expect("Failed to download");

    // Verify
    let downloaded = std::fs::read(download_file.path())
        .expect("Failed to read downloaded file");

    assert_eq!(blob, downloaded, "Downloaded data does not match uploaded data");

    println!("Success! Data verified.");
}

/// Test system initialization.
///
/// Verifies that the system can be initialized and basic
/// account queries work.
#[tokio::test]
#[ignore]
#[serial]
async fn test_system_init() {
    println!("=== System Init Test ===");

    // Setup without nodes - just validator and system init
    let ctx = TestContext::builder()
        .nodes(0)
        .build()
        .await
        .expect("Failed to setup test context");

    // Query system state
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.total_nodes, 0);

    // Query epoch state
    let epoch = ctx.epoch().await.expect("Failed to get epoch account");
    // Epoch ID is always set now - just check it's valid
    assert!(epoch.id.as_u64() >= 1, "Expected epoch ID to be set");
    assert!(epoch.state.is_active(), "Expected epoch to be in Active phase");

    println!("System initialized successfully");
}

/// Test node registration.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_registration() {
    println!("=== Node Registration Test ===");

    // Setup with 3 nodes (not bootstrapped - just registered)
    let ctx = TestContext::builder()
        .nodes(3)
        .port(8080)
        .build()
        .await
        .expect("Failed to setup test context");

    // Verify system state
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.total_nodes, 3);

    println!("All {} nodes registered successfully", ctx.nodes.len());
}

/// Test staking flow.
#[tokio::test]
#[ignore]
#[serial]
async fn test_staking_flow() {
    println!("=== Staking Flow Test ===");

    // Setup with 1 node, custom stake amount
    let ctx = TestContext::builder()
        .nodes(1)
        .port(8080)
        .stake(5000)
        .build()
        .await
        .expect("Failed to setup test context");

    // Check committee_next (node joined but not yet in committee)
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.committee_next.size(), 1);

    // Advance epoch to activate node
    ctx.wait_and_advance_epoch()
        .await
        .expect("Failed to advance epoch");

    // Check committee
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.committee.size(), 1);

    println!("Staking flow completed successfully");
}

/// Quick test for node registration, staking, and epoch advancement.
#[tokio::test]
#[ignore]
#[serial]
async fn test_with_running_validator() {
    println!("=== Quick Integration Test ===");

    // Full setup with bootstrap
    let ctx = TestContext::builder()
        .nodes(1)
        .port(9080)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Verify committee after bootstrap
    let system = ctx.system().await.expect("Failed to get system account");
    assert_eq!(system.committee.size(), 1);

    println!("Test passed! Node registered, staked, joined, and epoch advanced.");
}
