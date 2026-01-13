//! Basic end-to-end flow test.
//!
//! Tests the fundamental operations:
//! 1. Initialize system
//! 2. Register node
//! 3. Stake tokens
//! 4. Join committee
//! 5. Advance epoch
//! 6. Start node
//! 7. Upload data
//! 8. Download and verify
//!
//! Run with: `cargo test -p tape-e2e --test basic_flow`
//!
//! Note: Requires a local validator to be running (`make validator`).

use std::time::Duration;

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
#[ignore] // Run with --ignored flag, requires validator
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
    assert_eq!(system.total_tapes, Some(0));

    // Query epoch state
    let epoch = cli.account_epoch()
        .expect("Failed to get epoch account");

    assert_eq!(epoch.id, Some(0));
    assert_eq!(epoch.phase.as_deref(), Some("Active"));

    println!("System initialized successfully");
}

/// Test node registration.
#[tokio::test]
#[ignore]
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

/// Test that helps with manual debugging.
///
/// This doesn't assert anything - it just sets up the environment
/// and leaves it running for manual testing.
#[tokio::test]
#[ignore]
async fn test_setup_for_manual_testing() {
    println!("Setting up test environment...");

    // Assume validator is already running (via `make validator`)
    let cli = Tapedrive::new_localnet();

    // Initialize if needed
    match cli.admin_init() {
        Ok(_) => println!("System initialized"),
        Err(e) => println!("Init skipped (may already be initialized): {}", e),
    }

    // Create and setup a node
    let mut node = TestNode::new(0, 8080)
        .expect("Failed to create test node");

    match node.register(&cli) {
        Ok(addr) => println!("Node registered: {}", addr),
        Err(e) => println!("Registration skipped: {}", e),
    }

    match node.stake(&cli, 1000) {
        Ok(addr) => println!("Staked: {}", addr),
        Err(e) => println!("Stake skipped: {}", e),
    }

    match node.join(&cli) {
        Ok(_) => println!("Joined committee"),
        Err(e) => println!("Join skipped: {}", e),
    }

    match cli.admin_advance_epoch() {
        Ok(_) => println!("Epoch advanced"),
        Err(e) => println!("Epoch advance skipped: {}", e),
    }

    println!("\nNode config: {}", node.config_path.display());
    println!("Node URL: {}", node.url());
    println!("\nTo start the node manually:");
    println!("  tape -u l node start --config {}", node.config_path.display());

    // Keep the test running so files aren't cleaned up
    println!("\nPress Ctrl+C to exit...");
    tokio::signal::ctrl_c().await.ok();
}
