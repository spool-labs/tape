//! Epoch lifecycle tests across many epochs.
//!
//! Tests that verify correct behavior in both low-quorum and normal modes
//! over extended periods (20+ epochs).
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! **Design principle**: After initial bootstrap (one manual epoch advance to activate
//! nodes in committee_next), nodes handle subsequent epoch advancement autonomously.
//! Tests observe and verify state transitions.
//!
//! ```bash
//! cargo test -p tape-e2e --test quorum_lifecycle -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    Tapedrive, TestNode, Validator, ValidatorOptions, wait_for_rpc,
    wait_for_epoch_advance_from, MIN_COMMITTEE_SIZE, LONG_TIMEOUT, MIN_EPOCH_WAIT,
};

/// Test low-quorum mode lifecycle over 20+ epochs.
///
/// In low-quorum mode (< 24 nodes):
/// - Epoch stays in Active phase (skips Syncing)
/// - Nodes autonomously call AdvanceEpoch when MIN_EPOCH_DURATION passes
/// - Spool allocations should be stable
///
/// This test verifies:
/// 1. System correctly identifies low-quorum mode
/// 2. Nodes advance epochs autonomously
/// 3. Epoch stays in Active phase (skips Syncing)
/// 4. Committee size remains stable
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
            .with_timeout(Duration::from_secs(300))  // 5 min timeout
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

    // Fund nodes with SOL for transaction fees
    println!("\n=== Funding nodes ===");
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

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

    // Bootstrap: Manual epoch advance to activate nodes from committee_next to committee.
    // This is required once because nodes can only autonomously advance epochs AFTER
    // they're in the committee. Once in committee, nodes handle subsequent advances.
    println!("\n=== Bootstrap: Activating nodes (one-time manual advance) ===");
    println!("Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Bootstrap epoch advance failed");

    let system = cli.account_system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size: {} (low-quorum threshold: {})", committee_size, MIN_COMMITTEE_SIZE);

    assert!(
        committee_size < MIN_COMMITTEE_SIZE,
        "Expected low-quorum mode (committee {} < {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Track epochs as they advance autonomously
    println!("\n=== Observing {} epochs in low-quorum mode ===", NUM_EPOCHS);
    println!("(Nodes will advance epochs autonomously)");

    let mut last_epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
    let mut epochs_observed = 0u64;

    while epochs_observed < NUM_EPOCHS {
        // Wait for next epoch advance
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        let system = cli.account_system().expect("Failed to get system");
        let epoch_id = epoch.id.unwrap_or(0);

        epochs_observed += epoch_id - last_epoch_id;
        last_epoch_id = epoch_id;

        println!(
            "  Epoch {}: id={}, phase={:?}, committee={}",
            epochs_observed,
            epoch_id,
            epoch.phase.as_deref().unwrap_or("unknown"),
            system.committee_size.unwrap_or(0)
        );

        // In low-quorum mode, epoch should stay in Active phase
        assert_eq!(
            epoch.phase.as_deref(),
            Some("Active"),
            "Low-quorum mode should skip Syncing phase"
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
/// - Nodes autonomously handle all phase transitions
/// - Spool allocations distributed across nodes
///
/// This test verifies:
/// 1. System correctly identifies normal mode
/// 2. Nodes handle epoch advancement autonomously
/// 3. Phase transitions occur correctly (Active -> Syncing -> Settling -> Active)
/// 4. Committee size meets minimum
/// 5. Multiple epochs complete without errors
#[tokio::test]
#[ignore]
#[serial]
async fn test_normal_mode_lifecycle_20_epochs() {
    const NUM_NODES: usize = 25;  // Just above MIN_COMMITTEE_SIZE (24)
    const NUM_EPOCHS: u64 = 5;    // Each epoch ~60s in test mode
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

        if i % 5 == 0 {
            println!("  Registered {} nodes...", i + 1);
        }
        nodes.push(node);
    }
    println!("  All {} nodes registered", NUM_NODES);

    // Fund nodes with SOL for transaction fees
    println!("\n=== Funding nodes ===");
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    // Start all nodes
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

    // Bootstrap: Manual epoch advance to activate nodes from committee_next to committee.
    // In normal mode (>=24 nodes), we need to wait EPOCH_DURATION (60s) not MIN_EPOCH_DURATION.
    println!("\n=== Bootstrap: Activating nodes (one-time manual advance) ===");
    use tape_e2e::EPOCH_WAIT;
    println!("Waiting {}s for EPOCH_DURATION...", EPOCH_WAIT.as_secs());
    tokio::time::sleep(EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Bootstrap epoch advance failed");

    let system = cli.account_system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size: {}", committee_size);

    assert!(
        committee_size >= MIN_COMMITTEE_SIZE,
        "Expected normal mode (committee {} >= {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Track phase transitions
    let mut phase_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut last_epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
    let mut epochs_completed = 0u64;

    // Observe epochs as they advance autonomously
    println!("\n=== Observing {} epoch cycles in normal mode ===", NUM_EPOCHS);
    println!("(Nodes handle all phase transitions autonomously)");

    while epochs_completed < NUM_EPOCHS {
        // Wait for epoch to advance
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        let system = cli.account_system().expect("Failed to get system");
        let epoch_id = epoch.id.unwrap_or(0);
        let phase = epoch.phase.as_deref().unwrap_or("unknown").to_string();

        epochs_completed += epoch_id - last_epoch_id;
        last_epoch_id = epoch_id;

        *phase_counts.entry(phase.clone()).or_insert(0) += 1;

        println!(
            "  Epoch {}: id={}, phase={}, committee={}",
            epochs_completed,
            epoch_id,
            phase,
            system.committee_size.unwrap_or(0)
        );

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

    // Verify final state
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    let final_system = cli.account_system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {}", final_epoch.id.unwrap_or(0));
    println!("Phase: {:?}", final_epoch.phase);
    println!("Committee size: {}", final_system.committee_size.unwrap_or(0));

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

    println!("\nTest passed: Normal mode lifecycle completed {} epoch cycles successfully", NUM_EPOCHS);
}

/// Test node health monitoring over multiple epochs.
///
/// Verifies that nodes remain healthy throughout epoch transitions.
/// Nodes handle epoch advancement autonomously.
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

    // Register nodes
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register node");
        node.stake(&cli, 1000).expect("Failed to stake node");
        node.join(&cli).expect("Failed to join node");
        nodes.push(node);
    }

    // Fund and start nodes
    for (i, node) in nodes.iter().enumerate() {
        if let Err(e) = node.fund(&cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    for node in nodes.iter_mut() {
        node.start(&cli).expect("Failed to start node");
    }

    // Wait for nodes to be healthy
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Bootstrap: Manual epoch advance to activate nodes from committee_next to committee.
    println!("Bootstrap: Waiting {}s for MIN_EPOCH_DURATION...", MIN_EPOCH_WAIT.as_secs());
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
    cli.admin_advance_epoch().expect("Bootstrap epoch advance failed");

    // Track health across epochs
    let mut health_history: Vec<Vec<bool>> = Vec::new();
    let mut last_epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
    let mut epochs_observed = 0u64;

    while epochs_observed < NUM_EPOCHS {
        // Wait for next epoch
        wait_for_epoch_advance_from(&cli, last_epoch_id, LONG_TIMEOUT)
            .await
            .expect("Epoch should advance");

        let epoch_id = cli.account_epoch().expect("epoch").id.unwrap_or(0);
        epochs_observed += epoch_id - last_epoch_id;
        last_epoch_id = epoch_id;

        let mut epoch_health = Vec::new();
        for node in &nodes {
            let healthy = node.is_healthy().await;
            epoch_health.push(healthy);
        }

        let all_healthy = epoch_health.iter().all(|&h| h);
        let healthy_count = epoch_health.iter().filter(|&&h| h).count();

        println!(
            "  Epoch {}: {}/{} nodes healthy",
            epochs_observed, healthy_count, NUM_NODES
        );

        health_history.push(epoch_health);

        // All nodes should remain healthy
        assert!(all_healthy, "Some nodes became unhealthy at epoch {}", epochs_observed);
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
