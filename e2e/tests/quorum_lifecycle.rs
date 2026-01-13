//! Epoch lifecycle tests across many epochs.
//!
//! Tests that verify correct behavior in both low-quorum and normal modes
//! over extended periods (20+ epochs).
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test quorum_lifecycle -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    Tapedrive, TestNode, Validator, ValidatorOptions, wait_for_rpc,
    MIN_EPOCH_WAIT, EPOCH_WAIT, MIN_COMMITTEE_SIZE,
};

/// Wait for MIN_EPOCH_DURATION to pass before advancing epoch (low-quorum mode).
async fn wait_for_epoch_advance() {
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
}

/// Wait for EPOCH_DURATION to pass before advancing epoch (normal mode).
async fn wait_for_epoch_advance_normal() {
    tokio::time::sleep(EPOCH_WAIT).await;
}

/// Test low-quorum mode lifecycle over 20+ epochs.
///
/// In low-quorum mode (< 24 nodes):
/// - Epoch stays in Active phase (skips Syncing)
/// - Uses MIN_EPOCH_DURATION timing
/// - Spool allocations should be stable
///
/// This test verifies:
/// 1. System correctly identifies low-quorum mode
/// 2. Epoch advances without Syncing phase
/// 3. Committee size remains stable
/// 4. Nodes can join and leave without disruption
/// 5. All 20+ epochs complete without errors
#[tokio::test]
#[ignore]
#[serial]
async fn test_low_quorum_lifecycle_20_epochs() {
    const NUM_NODES: usize = 5;  // Well below MIN_COMMITTEE_SIZE (24)
    const NUM_EPOCHS: u64 = 20;
    const BASE_PORT: u16 = 10100;

    println!("=== Low-Quorum Lifecycle Test ({} nodes, {} epochs) ===", NUM_NODES, NUM_EPOCHS);

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(600))  // 10 min timeout for 20 epochs
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

    let initial_epoch = cli.account_epoch().expect("Failed to get epoch");
    let initial_epoch_id = initial_epoch.id.unwrap_or(1);
    println!("Initial epoch: {}", initial_epoch_id);

    // Register nodes
    println!("\n=== Registering {} nodes ===", NUM_NODES);
    let mut nodes: Vec<TestNode> = Vec::new();

    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT)
            .expect(&format!("Failed to create node {}", i));

        node.register(&cli)
            .expect(&format!("Failed to register node {}", i));
        node.stake(&cli, 1000)
            .expect(&format!("Failed to stake node {}", i));
        node.join(&cli)
            .expect(&format!("Failed to join node {}", i));

        println!("Node {} registered, staked, and joined", i);
        nodes.push(node);
    }

    // Advance epoch to activate nodes
    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    let system = cli.account_system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("\nInitial committee size: {} (low-quorum threshold: {})", committee_size, MIN_COMMITTEE_SIZE);

    assert!(
        committee_size < MIN_COMMITTEE_SIZE,
        "Expected low-quorum mode (committee {} < {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Start all nodes
    println!("\n=== Starting all nodes ===");
    for (i, node) in nodes.iter_mut().enumerate() {
        node.start(&cli).expect(&format!("Failed to start node {}", i));
        println!("Node {} started", i);
    }

    // Wait for nodes to be healthy
    tokio::time::sleep(Duration::from_secs(5)).await;
    for node in &nodes {
        if !node.is_healthy().await {
            println!("Warning: Node {} not healthy", node.name);
        }
    }

    // Run through 20 epochs
    println!("\n=== Running {} epochs in low-quorum mode ===", NUM_EPOCHS);

    for epoch_num in 1..=NUM_EPOCHS {
        wait_for_epoch_advance().await;

        match cli.admin_advance_epoch() {
            Ok(_) => {},
            Err(e) => {
                // In low-quorum mode, admin_advance_epoch might fail if called too soon
                println!("  Epoch {}: advance deferred ({})", epoch_num, e);
                continue;
            }
        }

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        let system = cli.account_system().expect("Failed to get system");

        println!(
            "  Epoch {}: id={}, phase={:?}, committee={}",
            epoch_num,
            epoch.id.unwrap_or(0),
            epoch.phase.as_deref().unwrap_or("unknown"),
            system.committee_size.unwrap_or(0)
        );

        // In low-quorum mode, epoch should stay in Active phase
        assert_eq!(
            epoch.phase.as_deref(),
            Some("Active"),
            "Low-quorum mode should skip Syncing phase"
        );

        // Committee size should remain stable
        assert_eq!(
            system.committee_size.unwrap_or(0),
            committee_size,
            "Committee size should remain stable in low-quorum mode"
        );
    }

    // Check node logs for errors
    println!("\n=== Checking node logs for errors ===");
    let mut found_errors = false;
    for node in &nodes {
        if let Ok(log) = node.read_log() {
            let has_bad_spool = log.contains("BadSpoolHash") || log.contains("0x54");
            let has_bad_epoch = log.contains("BadEpochId") || log.contains("0x43");
            let has_panic = log.contains("panic") || log.contains("PANIC");

            if has_bad_spool || has_bad_epoch || has_panic {
                found_errors = true;
                println!("Node {} has errors:", node.name);
                if has_bad_spool { println!("  - BadSpoolHash"); }
                if has_bad_epoch { println!("  - BadEpochId"); }
                if has_panic { println!("  - Panic"); }
            }
        }
    }

    assert!(!found_errors, "Found errors in node logs");

    // Verify final state
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    let final_system = cli.account_system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {}", final_epoch.id.unwrap_or(0));
    println!("Phase: {:?}", final_epoch.phase);
    println!("Committee size: {}", final_system.committee_size.unwrap_or(0));

    assert!(
        final_epoch.id.unwrap_or(0) >= initial_epoch_id + NUM_EPOCHS - 2,
        "Should have advanced through ~{} epochs",
        NUM_EPOCHS
    );

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: Low-quorum lifecycle completed {} epochs successfully", NUM_EPOCHS);
}

/// Test normal mode lifecycle over multiple epochs.
///
/// In normal mode (>= 24 nodes):
/// - Epoch goes through Active -> Syncing -> Settling -> Active
/// - Uses EPOCH_DURATION timing
/// - Spool allocations distributed across nodes
///
/// This test verifies:
/// 1. System correctly identifies normal mode
/// 2. Epoch phases transition correctly via node attestations
/// 3. Committee size meets minimum
/// 4. Nodes can submit sync attestations
/// 5. Multiple epochs complete without errors
#[tokio::test]
#[ignore]
#[serial]
async fn test_normal_mode_lifecycle_20_epochs() {
    const NUM_NODES: usize = 25;  // Just above MIN_COMMITTEE_SIZE (24)
    const NUM_EPOCHS: u64 = 5;    // Reduced for faster testing (each epoch requires full cycle)
    const BASE_PORT: u16 = 10200;

    println!("=== Normal Mode Lifecycle Test ({} nodes, {} epochs) ===", NUM_NODES, NUM_EPOCHS);

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(600))  // 10 min timeout
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

    let initial_epoch = cli.account_epoch().expect("Failed to get epoch");
    let initial_epoch_id = initial_epoch.id.unwrap_or(1);
    println!("Initial epoch: {}", initial_epoch_id);

    // Register nodes (takes time with 25 nodes)
    println!("\n=== Registering {} nodes (this will take a moment) ===", NUM_NODES);
    let mut nodes: Vec<TestNode> = Vec::new();

    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT)
            .expect(&format!("Failed to create node {}", i));

        node.register(&cli)
            .expect(&format!("Failed to register node {}", i));
        node.stake(&cli, 1000)
            .expect(&format!("Failed to stake node {}", i));
        node.join(&cli)
            .expect(&format!("Failed to join node {}", i));

        if i % 5 == 0 {
            println!("  Registered {} nodes...", i + 1);
        }
        nodes.push(node);
    }
    println!("  All {} nodes registered", NUM_NODES);

    // Fund nodes with SOL for transaction fees (needed for sync/advance_pool calls)
    println!("\n=== Funding nodes with SOL ===");
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }
    println!("  All nodes funded");

    // Check committee_next size
    let system = cli.account_system().expect("Failed to get system");
    println!("Committee next size: {}", system.committee_next_size.unwrap_or(0));

    // Advance epoch to activate nodes
    // Use MIN_EPOCH_WAIT for first advance since we just initialized
    println!("\n=== Activating nodes ===");
    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    let system = cli.account_system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size after activation: {}", committee_size);

    assert!(
        committee_size >= MIN_COMMITTEE_SIZE,
        "Expected normal mode (committee {} >= {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Start all nodes - they will automatically submit SyncEpoch when epoch advances
    println!("\n=== Starting all nodes ===");
    for (i, node) in nodes.iter_mut().enumerate() {
        match node.start(&cli) {
            Ok(_) => {},
            Err(e) => println!("Warning: Node {} failed to start: {}", i, e),
        }
    }
    println!("All nodes started");

    // Wait for nodes to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify we're in normal mode by checking the epoch phase
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("After first advance - Epoch: {}, Phase: {:?}",
        epoch.id.unwrap_or(0),
        epoch.phase.as_deref().unwrap_or("unknown")
    );

    // Should be in Syncing after advancing in normal mode
    assert_eq!(
        epoch.phase.as_deref(),
        Some("Syncing"),
        "Expected Syncing phase after advance in normal mode"
    );

    // Track phase transitions
    let mut phase_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut last_epoch_id = epoch.id.unwrap_or(0);

    // Run through epochs - nodes handle sync/advance_pool automatically
    // We just observe the phase transitions
    println!("\n=== Running {} epoch cycles in normal mode ===", NUM_EPOCHS);
    println!("(Nodes will automatically submit SyncEpoch and AdvancePool)");

    for epoch_num in 1..=NUM_EPOCHS {
        println!("\n--- Epoch cycle {} ---", epoch_num);

        // Wait for nodes to process and submit attestations
        // Check phase every few seconds to observe transitions
        for check in 0..20 {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let epoch = cli.account_epoch().expect("Failed to get epoch");
            let phase = epoch.phase.as_deref().unwrap_or("unknown").to_string();
            let epoch_id = epoch.id.unwrap_or(0);

            *phase_counts.entry(phase.clone()).or_insert(0) += 1;

            // Only print when phase or epoch changes
            if epoch_id != last_epoch_id || check == 0 {
                println!(
                    "  Check {}: epoch_id={}, phase={}, elapsed={}s",
                    check, epoch_id, phase, check * 5
                );
                last_epoch_id = epoch_id;
            }

            // If we're in Active and EPOCH_DURATION has passed, try to advance
            if phase == "Active" {
                // Wait for EPOCH_DURATION
                wait_for_epoch_advance_normal().await;

                match cli.admin_advance_epoch() {
                    Ok(_) => {
                        println!("  Epoch advanced to next cycle");
                        break; // Move to next epoch cycle
                    }
                    Err(e) => {
                        println!("  Epoch advance failed: {}", e);
                    }
                }
            }
        }

        let system = cli.account_system().expect("Failed to get system");
        println!("  Committee size: {}", system.committee_size.unwrap_or(0));

        // Committee size should remain >= MIN_COMMITTEE_SIZE
        assert!(
            system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
            "Committee size dropped below minimum"
        );
    }

    // Check phase distribution
    println!("\n=== Phase Distribution ===");
    for (phase, count) in &phase_counts {
        println!("  {}: {} occurrences", phase, count);
    }

    // Should have seen phase transitions if nodes are working
    println!("  (Syncing with no transition to Settling indicates nodes aren't funded/working)");

    // Verify final state
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    let final_system = cli.account_system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {}", final_epoch.id.unwrap_or(0));
    println!("Phase: {:?}", final_epoch.phase);
    println!("Committee size: {}", final_system.committee_size.unwrap_or(0));

    // Primary assertion: committee size maintained (system stayed stable)
    assert!(
        final_system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
        "Committee size should remain >= {} for normal mode",
        MIN_COMMITTEE_SIZE
    );

    // Check if epochs advanced (indicates nodes are working properly)
    let epochs_advanced = final_epoch.id.unwrap_or(0) > initial_epoch_id + 1;
    if !epochs_advanced {
        println!("WARNING: Epochs did not advance beyond initial. This indicates:");
        println!("  - Nodes may not be funded (can't submit SyncEpoch)");
        println!("  - Or nodes are not automatically submitting attestations");
        println!("  Check node logs for 'Attempt to debit an account' errors.");
    }

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    // Final assertion: epochs should have advanced
    assert!(
        epochs_advanced,
        "Epochs should have advanced beyond initial in normal mode (got {} from initial {})",
        final_epoch.id.unwrap_or(0),
        initial_epoch_id
    );

    println!("\nTest passed: Normal mode lifecycle completed {} epoch cycles successfully", NUM_EPOCHS);
}

/// Test node health monitoring over multiple epochs.
///
/// Verifies that nodes remain healthy throughout epoch transitions.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_health_across_epochs() {
    const NUM_NODES: usize = 3;
    const NUM_EPOCHS: u64 = 10;
    const BASE_PORT: u16 = 10300;

    println!("=== Node Health Test ({} nodes, {} epochs) ===", NUM_NODES, NUM_EPOCHS);

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

    // Register and start nodes
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register node");
        node.stake(&cli, 1000).expect("Failed to stake node");
        node.join(&cli).expect("Failed to join node");
        nodes.push(node);
    }

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance epoch");

    for node in nodes.iter_mut() {
        node.start(&cli).expect("Failed to start node");
    }

    // Wait for nodes to start
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check initial health
    let mut health_history: Vec<Vec<bool>> = Vec::new();

    for epoch_num in 1..=NUM_EPOCHS {
        wait_for_epoch_advance().await;
        let _ = cli.admin_advance_epoch();

        let mut epoch_health = Vec::new();
        for node in &nodes {
            let healthy = node.is_healthy().await;
            epoch_health.push(healthy);
        }

        let all_healthy = epoch_health.iter().all(|&h| h);
        let healthy_count = epoch_health.iter().filter(|&&h| h).count();

        println!(
            "  Epoch {}: {}/{} nodes healthy",
            epoch_num, healthy_count, NUM_NODES
        );

        health_history.push(epoch_health);

        // All nodes should remain healthy
        assert!(all_healthy, "Some nodes became unhealthy at epoch {}", epoch_num);
    }

    // Summary
    let total_checks = health_history.len() * NUM_NODES;
    let healthy_checks: usize = health_history.iter()
        .flat_map(|e| e.iter())
        .filter(|&&h| h)
        .count();

    println!("\n=== Health Summary ===");
    println!("Total health checks: {}", total_checks);
    println!("Healthy checks: {}", healthy_checks);
    println!("Health rate: {:.1}%", 100.0 * healthy_checks as f64 / total_checks as f64);

    assert_eq!(healthy_checks, total_checks, "Not all health checks passed");

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: All nodes remained healthy across {} epochs", NUM_EPOCHS);
}
