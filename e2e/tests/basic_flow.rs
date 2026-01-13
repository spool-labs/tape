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
    Tapedrive, TestNode, Validator, ValidatorOptions,
    wait_for_node_health, wait_for_rpc,
    temp_file_with_content, random_blob, sizes,
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
    // Setup - spawn validator
    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(120))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    // Create CLI wrapper
    let cli = Tapedrive::new_localnet();

    // Initialize system
    cli.admin_init()
        .expect("Failed to initialize system");

    // Create and register node
    let mut node = TestNode::new(0, 8080)
        .expect("Failed to create test node");

    let node_address = node.register(&cli)
        .expect("Failed to register node");

    println!("Registered node: {}", node_address);

    // Stake tokens
    let stake_address = node.stake(&cli, 1000)
        .expect("Failed to stake");

    println!("Staked to: {}", stake_address);

    // Join committee
    node.join(&cli)
        .expect("Failed to join committee");

    // Advance epoch to activate node
    cli.admin_advance_epoch()
        .expect("Failed to advance epoch");

    // Start node
    node.start(&cli)
        .expect("Failed to start node");

    // Wait for node to be healthy
    wait_for_node_health(&node.url(), Duration::from_secs(30))
        .await
        .expect("Node did not become healthy");

    println!("Node healthy at {}", node.url());

    // Create test data
    let blob = random_blob(sizes::SMALL);
    let upload_file = temp_file_with_content(&blob)
        .expect("Failed to create temp file");

    // Upload
    let upload_result = cli.storage_upload(
        upload_file.path(),
        None, // auto-create tape
        Some(&[node.url()]),
    ).expect("Failed to upload");

    println!("Uploaded track: {}", upload_result.track_id);

    // Download
    let download_file = tempfile::NamedTempFile::new()
        .expect("Failed to create download file");

    cli.storage_download(
        &upload_result.track_id,
        download_file.path(),
        Some(&[node.url()]),
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
    let validator = Validator::spawn()
        .await
        .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    // Initialize system
    cli.admin_init()
        .expect("Failed to initialize system");

    // Query system state
    let system = cli.account_system()
        .expect("Failed to get system account");

    assert_eq!(system.total_nodes, Some(0));
    // Note: total_tapes is not currently output by the CLI

    // Query epoch state
    let epoch = cli.account_epoch()
        .expect("Failed to get epoch account");

    // Epoch ID might be 0 or 1 depending on initialization
    assert!(epoch.id.is_some(), "Expected epoch ID to be set");
    assert_eq!(epoch.phase.as_deref(), Some("Active"));

    println!("System initialized successfully");
}

/// Test node registration.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_registration() {
    let validator = Validator::spawn()
        .await
        .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    cli.admin_init()
        .expect("Failed to initialize system");

    // Create and register multiple nodes
    let mut nodes = Vec::new();
    for i in 0..3 {
        let mut node = TestNode::new(i, 8080)
            .expect("Failed to create test node");

        let addr = node.register(&cli)
            .expect("Failed to register node");

        println!("Registered node {}: {}", i, addr);
        nodes.push(node);
    }

    // Verify system state
    let system = cli.account_system()
        .expect("Failed to get system account");

    assert_eq!(system.total_nodes, Some(3));

    println!("All nodes registered successfully");
}

/// Test staking flow.
#[tokio::test]
#[ignore]
#[serial]
async fn test_staking_flow() {
    let validator = Validator::spawn()
        .await
        .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    cli.admin_init()
        .expect("Failed to initialize system");

    let mut node = TestNode::new(0, 8080)
        .expect("Failed to create test node");

    node.register(&cli)
        .expect("Failed to register node");

    // Stake
    let stake_addr = node.stake(&cli, 5000)
        .expect("Failed to stake");

    println!("Stake account: {}", stake_addr);

    // Join committee
    node.join(&cli)
        .expect("Failed to join committee");

    // Check committee_next
    let system = cli.account_system()
        .expect("Failed to get system account");

    assert_eq!(system.committee_next_size, Some(1));

    // Advance epoch
    cli.admin_advance_epoch()
        .expect("Failed to advance epoch");

    // Check committee
    let system = cli.account_system()
        .expect("Failed to get system account");

    assert_eq!(system.committee_size, Some(1));

    println!("Staking flow completed successfully");
}

/// Quick test for node registration, staking, and epoch advancement.
#[tokio::test]
#[ignore]
#[serial]
async fn test_with_running_validator() {
    let _validator = Validator::spawn()
        .await
        .expect("Failed to spawn validator");

    let cli = Tapedrive::new_localnet();

    cli.admin_init()
        .expect("Failed to initialize system");
    println!("System initialized");

    // Create and register a test node
    let mut node = TestNode::new(0, 9080)
        .expect("Failed to create test node");

    println!("\nCreated test node:");
    println!("  Config: {}", node.config_path.display());
    println!("  URL: {}", node.url());

    let node_addr = node.register(&cli)
        .expect("Failed to register node");
    println!("  Registered: {}", node_addr);

    // Stake
    let stake_addr = node.stake(&cli, 1000)
        .expect("Failed to stake");
    println!("  Staked: {}", stake_addr);

    // Join
    node.join(&cli)
        .expect("Failed to join committee");
    println!("  Joined committee");

    // Advance epoch
    cli.admin_advance_epoch()
        .expect("Failed to advance epoch");
    println!("  Epoch advanced");

    println!("\nTest passed! Node registered, staked, joined, and epoch advanced.");
}
