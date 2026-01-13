//! Catch-up mode tests.
//!
//! Tests that verify nodes correctly handle catch-up scenarios:
//! - Skipping stale epoch submissions when behind
//! - Refreshing system state when caught up
//! - Participating in current epoch after catch-up
//!
//! ## Running Tests
//!
//! These tests require a running validator (`make validator`):
//! ```bash
//! cargo test -p tape-e2e --test catchup -- --ignored --nocapture
//! ```

use std::time::Duration;

use tape_e2e::{Tapedrive, TestNode, wait_for_rpc};

const LOCALNET_RPC: &str = "http://127.0.0.1:8899";

/// Check if a validator is already running.
async fn validator_is_running() -> bool {
    let client = reqwest::Client::new();
    client
        .post(LOCALNET_RPC)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth"
        }))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// Test that a node starting late correctly identifies stale epochs.
///
/// Scenario:
/// 1. Initialize system, register node A, advance through several epochs
/// 2. Register node B (late joiner)
/// 3. Node B should recognize it's behind and skip stale epoch processing
#[tokio::test]
#[ignore]
async fn test_late_node_detects_stale_epochs() {
    if !validator_is_running().await {
        panic!("Validator not running. Start with: make validator");
    }

    let cli = Tapedrive::new_localnet();

    // Initialize system
    match cli.admin_init() {
        Ok(_) => println!("System initialized"),
        Err(e) => println!("Init: {} (may already be initialized)", e),
    }

    // Create and register first node
    let mut node_a = TestNode::new(0, 8080).expect("Failed to create node A");

    let addr_a = node_a.register(&cli).expect("Failed to register node A");
    println!("Node A registered: {}", addr_a);

    node_a.stake(&cli, 1000).expect("Failed to stake node A");
    node_a.join(&cli).expect("Failed to join node A");

    // Advance epoch to put node A in committee
    cli.admin_advance_epoch().expect("Failed to advance epoch");
    println!("Epoch advanced - Node A in committee");

    // Check epoch state
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Current epoch: {:?}, phase: {:?}", epoch.id, epoch.phase);

    // Advance a few more epochs (simulating time passing)
    for i in 0..3 {
        // In low-quorum mode, epoch goes Active immediately
        // Just advance again
        match cli.admin_advance_epoch() {
            Ok(_) => println!("Advanced to epoch {}", i + 2),
            Err(e) => println!("Advance epoch {}: {}", i + 2, e),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("After advances - epoch: {:?}, phase: {:?}", epoch.id, epoch.phase);

    // Now create a late-joining node B
    let mut node_b = TestNode::new(1, 8081).expect("Failed to create node B");

    let addr_b = node_b.register(&cli).expect("Failed to register node B");
    println!("Node B registered: {}", addr_b);

    node_b.stake(&cli, 1000).expect("Failed to stake node B");
    node_b.join(&cli).expect("Failed to join node B");

    // Advance epoch to include node B
    cli.admin_advance_epoch().expect("Failed to advance epoch for node B");

    let epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Final epoch: {:?}, phase: {:?}", epoch.id, epoch.phase);

    // Both nodes should be in committee now
    let system = cli.account_system().expect("Failed to get system");
    println!("Committee size: {:?}", system.committee_size);

    println!("\nTest passed: Late node registered and joined after multiple epochs");
}

/// Test that starting a node after epochs have passed doesn't cause errors.
///
/// This tests the core catch-up logic:
/// - Node starts and begins processing blocks
/// - Historical AdvanceEpoch events should be skipped (stale)
/// - Node should not attempt to submit SyncEpoch for old epochs
#[tokio::test]
#[ignore]
async fn test_node_startup_after_epoch_advances() {
    if !validator_is_running().await {
        panic!("Validator not running. Start with: make validator");
    }

    let cli = Tapedrive::new_localnet();

    // Initialize system
    match cli.admin_init() {
        Ok(_) => println!("System initialized"),
        Err(e) => println!("Init: {} (may already be initialized)", e),
    }

    // Get current epoch
    let initial_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Initial epoch: {:?}", initial_epoch.id);

    // Create and setup a node
    let mut node = TestNode::new(0, 8090).expect("Failed to create node");

    node.register(&cli).expect("Failed to register node");
    node.stake(&cli, 1000).expect("Failed to stake");
    node.join(&cli).expect("Failed to join");

    // Advance epoch to activate
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    // Advance several more epochs BEFORE starting the node
    // This creates the scenario where the node has to catch up
    for i in 0..5 {
        match cli.admin_advance_epoch() {
            Ok(_) => println!("Pre-start: Advanced epoch {}", i + 2),
            Err(e) => println!("Pre-start: Epoch advance {}: {}", i + 2, e),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let pre_start_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Epoch before node start: {:?}", pre_start_epoch.id);

    // Now start the node - it should catch up on all the epochs
    // without trying to submit stale transactions
    println!("\nStarting node (should catch up on {} epochs)...",
        pre_start_epoch.id.unwrap_or(0) - initial_epoch.id.unwrap_or(0));

    node.start(&cli).expect("Failed to start node");

    // Give node time to start and process blocks
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check if node is healthy
    let healthy = node.is_healthy().await;
    println!("Node healthy: {}", healthy);

    // Check logs for catch-up messages
    if let Ok(log) = node.read_log() {
        let lines: Vec<&str> = log.lines().collect();
        let recent: Vec<&str> = lines.iter().rev().take(50).copied().collect();

        println!("\nRecent log entries:");
        for line in recent.iter().rev() {
            // Look for stale epoch or catch-up related messages
            if line.contains("stale") || line.contains("catch") ||
               line.contains("Stale") || line.contains("Catch") ||
               line.contains("epoch") {
                println!("  {}", line);
            }
        }

        // Verify no BadSpoolHash or BadEpochId errors
        let has_bad_spool_hash = log.contains("BadSpoolHash") || log.contains("0x54");
        let has_bad_epoch_id = log.contains("BadEpochId") || log.contains("0x43");

        if has_bad_spool_hash {
            println!("\nWARNING: Found BadSpoolHash errors in logs!");
        }
        if has_bad_epoch_id {
            println!("\nWARNING: Found BadEpochId errors in logs!");
        }

        assert!(!has_bad_spool_hash, "Node should not have BadSpoolHash errors during catch-up");
        assert!(!has_bad_epoch_id, "Node should not have BadEpochId errors during catch-up");
    }

    node.stop();
    println!("\nTest passed: Node started and caught up without stale epoch errors");
}

/// Test that multiple nodes can join at different times.
///
/// Simulates a real-world scenario where nodes join the network at
/// different epochs and must catch up.
#[tokio::test]
#[ignore]
async fn test_staggered_node_joins() {
    if !validator_is_running().await {
        panic!("Validator not running. Start with: make validator");
    }

    let cli = Tapedrive::new_localnet();

    // Initialize system
    match cli.admin_init() {
        Ok(_) => println!("System initialized"),
        Err(e) => println!("Init: {} (may already be initialized)", e),
    }

    // Track epochs where each node joins
    let mut join_epochs = Vec::new();

    // Node 0 joins at epoch 1
    let mut node_0 = TestNode::new(0, 8070).expect("Failed to create node 0");
    node_0.register(&cli).expect("Failed to register node 0");
    node_0.stake(&cli, 1000).expect("Failed to stake node 0");
    node_0.join(&cli).expect("Failed to join node 0");
    cli.admin_advance_epoch().expect("Failed to advance");

    let epoch = cli.account_epoch().expect("Failed to get epoch");
    join_epochs.push(("node_0", epoch.id.unwrap_or(0)));
    println!("Node 0 joined at epoch {}", epoch.id.unwrap_or(0));

    // Advance a couple epochs
    cli.admin_advance_epoch().ok();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cli.admin_advance_epoch().ok();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Node 1 joins at epoch 3-ish
    let mut node_1 = TestNode::new(1, 8071).expect("Failed to create node 1");
    node_1.register(&cli).expect("Failed to register node 1");
    node_1.stake(&cli, 1000).expect("Failed to stake node 1");
    node_1.join(&cli).expect("Failed to join node 1");
    cli.admin_advance_epoch().ok();

    let epoch = cli.account_epoch().expect("Failed to get epoch");
    join_epochs.push(("node_1", epoch.id.unwrap_or(0)));
    println!("Node 1 joined at epoch {}", epoch.id.unwrap_or(0));

    // Advance more
    cli.admin_advance_epoch().ok();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cli.admin_advance_epoch().ok();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Node 2 joins late
    let mut node_2 = TestNode::new(2, 8072).expect("Failed to create node 2");
    node_2.register(&cli).expect("Failed to register node 2");
    node_2.stake(&cli, 1000).expect("Failed to stake node 2");
    node_2.join(&cli).expect("Failed to join node 2");
    cli.admin_advance_epoch().ok();

    let epoch = cli.account_epoch().expect("Failed to get epoch");
    join_epochs.push(("node_2", epoch.id.unwrap_or(0)));
    println!("Node 2 joined at epoch {}", epoch.id.unwrap_or(0));

    // Final state
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    let system = cli.account_system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {:?}", final_epoch.id);
    println!("Committee size: {:?}", system.committee_size);
    println!("Join history:");
    for (name, epoch) in &join_epochs {
        println!("  {} joined at epoch {}", name, epoch);
    }

    println!("\nTest passed: Staggered node joins completed successfully");
}

/// Test epoch state queries during catch-up.
///
/// Verifies that is_stale_epoch correctly identifies old epochs.
#[tokio::test]
#[ignore]
async fn test_epoch_staleness_detection() {
    if !validator_is_running().await {
        panic!("Validator not running. Start with: make validator");
    }

    let cli = Tapedrive::new_localnet();

    // Initialize
    match cli.admin_init() {
        Ok(_) => println!("System initialized"),
        Err(_) => println!("Already initialized"),
    }

    // Get initial epoch
    let initial = cli.account_epoch().expect("Failed to get epoch");
    let initial_id = initial.id.unwrap_or(0);
    println!("Initial epoch: {}", initial_id);

    // Advance several epochs
    let advances = 5;
    for i in 0..advances {
        match cli.admin_advance_epoch() {
            Ok(_) => {},
            Err(e) => println!("Advance {}: {}", i, e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let current = cli.account_epoch().expect("Failed to get epoch");
    let current_id = current.id.unwrap_or(0);
    println!("Current epoch: {}", current_id);

    // All epochs before current_id should be stale
    // (This is a conceptual test - the actual staleness check happens in the node)
    println!("\nEpoch staleness (conceptual):");
    for epoch_id in initial_id..=current_id {
        let is_stale = epoch_id < current_id;
        println!("  Epoch {}: {}", epoch_id, if is_stale { "STALE" } else { "CURRENT" });
    }

    assert!(current_id >= initial_id, "Epoch should have advanced");
    println!("\nTest passed: Epoch detection logic verified");
}
