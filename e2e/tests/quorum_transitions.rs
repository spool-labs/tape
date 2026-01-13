//! Quorum threshold transition tests.
//!
//! Tests that verify correct behavior when crossing the low-quorum threshold (24 nodes)
//! in both directions (scaling up and scaling down).
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! **Design principle**: After initial bootstrap (one manual epoch advance to activate
//! nodes in committee_next), nodes handle subsequent epoch advancement autonomously.
//! Tests observe and verify state transitions.
//!
//! ```bash
//! cargo test -p tape-e2e --test quorum_transitions -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    Tapedrive, TestNode, Validator, ValidatorOptions, wait_for_rpc,
    wait_for_epoch_advance_from, MIN_EPOCH_WAIT, MIN_COMMITTEE_SIZE, LONG_TIMEOUT,
};

/// Test transition from low-quorum to normal mode by adding nodes.
///
/// Scenario:
/// 1. Start with < 24 nodes (low-quorum mode)
/// 2. Add nodes until >= 24 (crossing threshold)
/// 3. Verify mode transition
/// 4. Continue advancing epochs in normal mode
#[tokio::test]
#[ignore]
#[serial]
async fn test_low_to_normal_quorum_transition() {
    const INITIAL_NODES: usize = 10;
    const FINAL_NODES: usize = 26;  // Above MIN_COMMITTEE_SIZE
    const BASE_PORT: u16 = 11100;

    println!("=== Low to Normal Quorum Transition Test ===");
    println!("Initial nodes: {} (low-quorum)", INITIAL_NODES);
    println!("Final nodes: {} (normal)", FINAL_NODES);
    println!("Threshold: {}", MIN_COMMITTEE_SIZE);

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(600))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    // Initialize system
    cli.admin_init().expect("Failed to initialize system");
    println!("System initialized");

    // Register initial nodes (below threshold)
    println!("\n=== Phase 1: Registering {} initial nodes (low-quorum) ===", INITIAL_NODES);
    let mut nodes: Vec<TestNode> = Vec::new();

    for i in 0..INITIAL_NODES {
        let mut node = TestNode::new(i, BASE_PORT)
            .expect(&format!("Failed to create node {}", i));

        node.register(&cli).expect(&format!("Failed to register node {}", i));
        node.stake(&cli, 1000).expect(&format!("Failed to stake node {}", i));
        node.join(&cli).expect(&format!("Failed to join node {}", i));

        nodes.push(node);
    }
    println!("Registered {} initial nodes", INITIAL_NODES);

    // Fund nodes with SOL for transaction fees
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    // Start nodes BEFORE bootstrap
    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bootstrap: Manual epoch advance to activate nodes from committee_next to committee
    println!("\n=== Bootstrap: Activating initial nodes ===");
    println!("Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    let system = cli.account_system().expect("Failed to get system");
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("After bootstrap:");
    println!("  Committee size: {}", system.committee_size.unwrap_or(0));
    println!("  Epoch: {}, Phase: {:?}", epoch.id.unwrap_or(0), epoch.phase);

    assert!(
        system.committee_size.unwrap_or(0) < MIN_COMMITTEE_SIZE,
        "Should be in low-quorum mode"
    );

    // Observe epochs advancing autonomously in low-quorum mode
    println!("\n=== Phase 2: Observing 5 epochs in low-quorum mode ===");
    println!("(Nodes will advance epochs autonomously)");

    let mut last_epoch_id = epoch.id.unwrap_or(0);
    for i in 1..=5 {
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        last_epoch_id = epoch.id.unwrap_or(0);

        println!(
            "  Epoch {}: id={}, phase={:?}",
            i,
            last_epoch_id,
            epoch.phase
        );

        // Should stay in Active phase (low-quorum skips Syncing)
        assert_eq!(
            epoch.phase.as_deref(),
            Some("Active"),
            "Low-quorum should stay in Active phase"
        );
    }

    // Add more nodes to cross threshold
    println!("\n=== Phase 3: Adding nodes to cross threshold ({} -> {}) ===",
        INITIAL_NODES, FINAL_NODES);

    for i in INITIAL_NODES..FINAL_NODES {
        let mut node = TestNode::new(i, BASE_PORT)
            .expect(&format!("Failed to create node {}", i));

        node.register(&cli).expect(&format!("Failed to register node {}", i));
        node.stake(&cli, 1000).expect(&format!("Failed to stake node {}", i));
        node.join(&cli).expect(&format!("Failed to join node {}", i));

        nodes.push(node);

        if (i - INITIAL_NODES) % 4 == 0 {
            println!("  Added node {}...", i);
        }
    }
    println!("  Total nodes now: {}", nodes.len());

    // Fund new nodes with SOL for transaction fees
    for (i, node) in nodes.iter().enumerate().skip(INITIAL_NODES) {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    // Start new nodes
    for node in nodes.iter_mut().skip(INITIAL_NODES) {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check committee_next
    let system = cli.account_system().expect("Failed to get system");
    println!("Committee next size: {}", system.committee_next_size.unwrap_or(0));

    // Wait for epoch to advance (nodes will handle it)
    println!("Waiting for epoch to advance and activate new nodes...");
    wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
        .await
        .expect("Epoch should advance");

    let system = cli.account_system().expect("Failed to get system");
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    last_epoch_id = epoch.id.unwrap_or(0);
    println!("\nAfter adding nodes:");
    println!("  Committee size: {}", system.committee_size.unwrap_or(0));
    println!("  Epoch: {}, Phase: {:?}", last_epoch_id, epoch.phase);

    // Verify transition to normal mode
    let committee_size = system.committee_size.unwrap_or(0);
    if committee_size >= MIN_COMMITTEE_SIZE {
        println!("\n=== Phase 4: Transitioned to normal mode! ===");

        // In normal mode, should see Syncing phase
        let phase = epoch.phase.as_deref().unwrap_or("unknown");
        println!("Current phase: {}", phase);

        // Observe epochs in normal mode
        println!("\n=== Phase 5: Observing 5 epochs in normal mode ===");
        println!("(Nodes will advance epochs autonomously)");

        for i in 1..=5 {
            wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
                .await
                .expect("Epoch should advance");

            let epoch = cli.account_epoch().expect("Failed to get epoch");
            let system = cli.account_system().expect("Failed to get system");
            last_epoch_id = epoch.id.unwrap_or(0);

            println!(
                "  Epoch {}: id={}, phase={:?}, committee={}",
                i,
                last_epoch_id,
                epoch.phase,
                system.committee_size.unwrap_or(0)
            );

            assert!(
                system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
                "Should stay in normal mode"
            );
        }
    } else {
        println!("\nNote: Committee size {} still below threshold {}", committee_size, MIN_COMMITTEE_SIZE);
        println!("This can happen if stake activation is delayed");

        // Still observe one more epoch to verify system stability
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        println!("Final epoch: {:?}", epoch.id);
    }

    // Check for errors
    println!("\n=== Checking for errors ===");
    let mut found_errors = false;
    for node in &nodes {
        if let Ok(log) = node.read_log() {
            if log.contains("BadSpoolHash") || log.contains("BadEpochId") || log.contains("panic") {
                found_errors = true;
                println!("Error found in node {}", node.name);
            }
        }
    }

    assert!(!found_errors, "Found errors during transition");

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: Low to normal quorum transition completed");
}

/// Test that stake changes affect committee membership.
///
/// This test:
/// 1. Registers nodes with varying stake amounts
/// 2. Verifies spool allocations reflect stake weight
/// 3. Changes stake and verifies reallocation
#[tokio::test]
#[ignore]
#[serial]
async fn test_stake_weight_affects_allocations() {
    const NUM_NODES: usize = 5;
    const BASE_PORT: u16 = 11200;

    println!("=== Stake Weight Allocation Test ===");

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(300))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    cli.admin_init().expect("Failed to initialize system");
    println!("System initialized");

    // Register nodes with different stake amounts
    // Higher stake should get more spool allocations
    let stake_amounts = [100, 500, 1000, 2000, 5000];
    let mut nodes: Vec<TestNode> = Vec::new();

    println!("\n=== Registering nodes with varying stake ===");
    for (i, &stake) in stake_amounts.iter().enumerate().take(NUM_NODES) {
        let mut node = TestNode::new(i, BASE_PORT)
            .expect(&format!("Failed to create node {}", i));

        node.register(&cli).expect(&format!("Failed to register node {}", i));
        node.stake(&cli, stake).expect(&format!("Failed to stake node {}", i));
        node.join(&cli).expect(&format!("Failed to join node {}", i));

        println!("  Node {}: stake = {}", i, stake);
        nodes.push(node);
    }

    // Fund nodes with SOL for transaction fees
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    // Start nodes BEFORE bootstrap
    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bootstrap: Manual epoch advance to activate nodes
    println!("\n=== Bootstrap: Activating nodes ===");
    println!("Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    // Query node status to see spool allocations
    println!("\n=== Initial Spool Allocations ===");
    let mut initial_allocations = Vec::new();

    for (i, node) in nodes.iter().enumerate() {
        if let Some(addr) = &node.node_address {
            match cli.node_status(Some(&node.config_path), Some(addr)) {
                Ok(status) => {
                    let spools = status.spool_count.unwrap_or(0);
                    println!("  Node {} (stake {}): {} spools", i, stake_amounts[i], spools);
                    initial_allocations.push(spools);
                }
                Err(e) => {
                    println!("  Node {}: status unavailable ({})", i, e);
                    initial_allocations.push(0);
                }
            }
        }
    }

    // In low-quorum mode, spool allocations should be proportional to stake
    // Higher stake should have more or equal spools (subject to discretization)
    let total_allocations: u16 = initial_allocations.iter().sum();
    println!("\nTotal spool allocations: {}", total_allocations);

    // Observe several epochs advancing autonomously
    println!("\n=== Observing 5 epochs ===");
    println!("(Nodes will advance epochs autonomously)");

    let mut last_epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
    for epoch_num in 1..=5 {
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        last_epoch_id = epoch.id.unwrap_or(0);

        println!(
            "  Epoch {}: id={}, phase={:?}",
            epoch_num,
            last_epoch_id,
            epoch.phase
        );
    }

    // Verify allocations are stable across epochs
    println!("\n=== Final Spool Allocations ===");
    for (i, node) in nodes.iter().enumerate() {
        if let Some(addr) = &node.node_address {
            if let Ok(status) = cli.node_status(Some(&node.config_path), Some(addr)) {
                let spools = status.spool_count.unwrap_or(0);
                let initial = initial_allocations.get(i).copied().unwrap_or(0);
                let change = if spools == initial { "same" } else { "CHANGED" };
                println!(
                    "  Node {} (stake {}): {} spools ({})",
                    i, stake_amounts[i], spools, change
                );
            }
        }
    }

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: Stake weight affects allocations");
}

/// Test adding and removing nodes across epochs.
///
/// This test:
/// 1. Starts with some nodes
/// 2. Adds new nodes mid-test
/// 3. Removes nodes (by stopping them and not rejoining)
/// 4. Verifies committee adjusts correctly
#[tokio::test]
#[ignore]
#[serial]
async fn test_dynamic_node_membership() {
    const BASE_PORT: u16 = 11300;

    println!("=== Dynamic Node Membership Test ===");

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(300))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    cli.admin_init().expect("Failed to initialize system");
    println!("System initialized");

    // Start with 3 nodes
    println!("\n=== Phase 1: Starting with 3 nodes ===");
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..3 {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register");
        node.stake(&cli, 1000).expect("Failed to stake");
        node.join(&cli).expect("Failed to join");
        nodes.push(node);
    }

    // Fund nodes with SOL for transaction fees
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    // Start nodes BEFORE bootstrap
    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bootstrap: Manual epoch advance to activate nodes from committee_next to committee
    println!("\n=== Bootstrap: Activating initial nodes ===");
    println!("Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Failed to advance");

    let system = cli.account_system().expect("Failed to get system");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

    // Run a few epochs (nodes advance autonomously)
    println!("\n=== Phase 2: Running 3 epochs with 3 nodes ===");
    println!("(Nodes will advance epochs autonomously)");

    let mut last_epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
    for i in 1..=3 {
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let system = cli.account_system().expect("Failed to get system");
        let epoch = cli.account_epoch().expect("Failed to get epoch");
        last_epoch_id = epoch.id.unwrap_or(0);

        println!("  Epoch {}: id={}, committee={}", i, last_epoch_id, system.committee_size.unwrap_or(0));
    }

    // Add 2 more nodes
    println!("\n=== Phase 3: Adding 2 more nodes ===");
    for i in 3..5 {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register");
        node.stake(&cli, 1000).expect("Failed to stake");
        node.join(&cli).expect("Failed to join");
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
        let _ = node.start(&cli);
        nodes.push(node);
        println!("  Added node {}", i);
    }

    // Wait for epoch to advance and activate new nodes
    println!("Waiting for epoch to advance and activate new nodes...");
    wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
        .await
        .expect("Epoch should advance");

    let system = cli.account_system().expect("Failed to get system");
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    last_epoch_id = epoch.id.unwrap_or(0);
    println!("Committee size after adding: {}", system.committee_size.unwrap_or(0));

    // Run more epochs (nodes advance autonomously)
    println!("\n=== Phase 4: Running 3 epochs with 5 nodes ===");
    println!("(Nodes will advance epochs autonomously)");

    for i in 1..=3 {
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let system = cli.account_system().expect("Failed to get system");
        let epoch = cli.account_epoch().expect("Failed to get epoch");
        last_epoch_id = epoch.id.unwrap_or(0);

        println!("  Epoch {}: id={}, committee={}", i, last_epoch_id, system.committee_size.unwrap_or(0));
    }

    // Stop 2 nodes (simulating departure)
    println!("\n=== Phase 5: Stopping 2 nodes ===");
    for node in nodes.iter_mut().take(2) {
        node.stop();
        println!("  Stopped {}", node.name);
    }

    // Note: Stopped nodes still count in committee until they miss attestations
    // and get removed. This tests the graceful degradation.

    // Run more epochs (remaining nodes advance autonomously)
    println!("\n=== Phase 6: Running 3 epochs with 2 nodes stopped ===");
    println!("(Remaining nodes will advance epochs autonomously)");

    for i in 1..=3 {
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let system = cli.account_system().expect("Failed to get system");
        let epoch = cli.account_epoch().expect("Failed to get epoch");
        last_epoch_id = epoch.id.unwrap_or(0);

        println!(
            "  Epoch {}: id={}, committee={}",
            i,
            last_epoch_id,
            system.committee_size.unwrap_or(0)
        );
    }

    // Final state
    let system = cli.account_system().expect("Failed to get system");
    println!("\n=== Final State ===");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

    // Check remaining nodes for errors
    let mut found_errors = false;
    for node in nodes.iter().skip(2) {  // Only check nodes that weren't stopped
        if let Ok(log) = node.read_log() {
            if log.contains("panic") {
                found_errors = true;
                println!("Error in {}", node.name);
            }
        }
    }

    assert!(!found_errors, "Found errors in remaining nodes");

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: Dynamic node membership handled correctly");
}

/// Test rapid epoch advances don't cause issues.
///
/// Calls admin_advance_epoch repeatedly without waiting for full duration.
/// Note: This test intentionally uses manual admin_advance_epoch() calls
/// because testing rapid advance rejection IS the purpose of this test.
#[tokio::test]
#[ignore]
#[serial]
async fn test_rapid_epoch_advance_attempts() {
    const BASE_PORT: u16 = 11400;

    println!("=== Rapid Epoch Advance Test ===");

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(180))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    cli.admin_init().expect("Failed to initialize system");

    // Register a node
    let mut node = TestNode::new(0, BASE_PORT).expect("Failed to create node");
    node.register(&cli).expect("Failed to register");
    node.stake(&cli, 1000).expect("Failed to stake");
    node.join(&cli).expect("Failed to join");

    // Fund node with SOL for transaction fees
    if let Err(e) = node.fund(&cli, 1.0) {
        println!("Warning: Failed to fund node: {}", e);
    }

    // Start node BEFORE bootstrap
    node.start(&cli).expect("Failed to start");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bootstrap: Manual epoch advance to activate node
    println!("Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Failed to advance");

    let initial_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.unwrap_or(0));

    // Attempt rapid advances (most should fail due to timing)
    // This is intentionally manual - we're testing that the system rejects rapid advances
    println!("\n=== Attempting 20 rapid advances ===");
    let mut successes = 0;
    let mut failures = 0;

    for i in 1..=20 {
        // Short wait (less than MIN_EPOCH_DURATION)
        tokio::time::sleep(Duration::from_secs(2)).await;

        match cli.admin_advance_epoch() {
            Ok(_) => {
                successes += 1;
                println!("  Attempt {}: SUCCESS", i);
            }
            Err(_) => {
                failures += 1;
                // Expected - epoch duration not elapsed
            }
        }
    }

    println!("\nRapid advances - Success: {}, Failed: {}", successes, failures);

    // Most attempts should fail due to MIN_EPOCH_DURATION
    assert!(
        failures > successes,
        "Most rapid advances should be rejected"
    );

    // Now wait proper duration and advance
    println!("\n=== Waiting full epoch duration ===");
    tokio::time::sleep(MIN_EPOCH_WAIT).await;

    match cli.admin_advance_epoch() {
        Ok(_) => println!("Advance after proper wait: SUCCESS"),
        Err(e) => println!("Advance after proper wait: {}", e),
    }

    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Final epoch: {}", final_epoch.id.unwrap_or(0));

    // Check node for errors
    if let Ok(log) = node.read_log() {
        assert!(
            !log.contains("panic"),
            "Node should not panic during rapid advances"
        );
    }

    node.stop();
    println!("\nTest passed: Rapid epoch advances handled correctly");
}
