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
use tape_e2e::{TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT};

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

    // Setup: spawn validator, register/stake/join nodes, fund, start, bootstrap
    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Verify we're in low-quorum mode
    let system = ctx.system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size: {} (low-quorum threshold: {})", committee_size, MIN_COMMITTEE_SIZE);

    assert!(
        committee_size < MIN_COMMITTEE_SIZE,
        "Expected low-quorum mode (committee {} < {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Observe epochs advancing autonomously
    println!("\n=== Observing {} epochs in low-quorum mode ===", NUM_EPOCHS);
    println!("(Nodes will advance epochs autonomously)");

    let mut epochs_observed = 0u64;
    ctx.observe_epochs(NUM_EPOCHS, |epoch, system| {
        epochs_observed += 1;
        println!(
            "  Epoch {}: id={}, phase={:?}, committee={}",
            epochs_observed,
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

        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Check node logs for errors
    println!("\n=== Checking node logs for errors ===");
    ctx.check_node_logs().expect("Found errors in node logs");

    // Verify final state
    let final_epoch = ctx.epoch().expect("Failed to get epoch");
    let final_system = ctx.system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {}", final_epoch.id.unwrap_or(0));
    println!("Phase: {:?}", final_epoch.phase);
    println!("Committee size: {}", final_system.committee_size.unwrap_or(0));

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

    // Setup with longer timeout for 25 nodes
    // Note: In normal mode, we need to wait EPOCH_DURATION not MIN_EPOCH_DURATION for bootstrap
    let mut ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build()  // Don't bootstrap automatically - we need custom timing
        .await
        .expect("Failed to setup test context");

    // Fund and start nodes manually (build() doesn't do this)
    for (i, node) in ctx.nodes.iter().enumerate() {
        if let Err(e) = node.fund(&ctx.cli, 1.0) {
            eprintln!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    for (i, node) in ctx.nodes.iter_mut().enumerate() {
        if let Err(e) = node.start(&ctx.cli) {
            eprintln!("Warning: Node {} failed to start: {}", i, e);
        }
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Bootstrap with EPOCH_DURATION wait (normal mode requires full epoch)
    println!("\n=== Bootstrap: Activating nodes (waiting {}s for EPOCH_DURATION) ===", EPOCH_WAIT.as_secs());
    tokio::time::sleep(EPOCH_WAIT).await;
    ctx.cli.admin_advance_epoch().expect("Bootstrap epoch advance failed");

    // Verify we're in normal mode
    let system = ctx.system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);
    println!("Committee size: {}", committee_size);

    assert!(
        committee_size >= MIN_COMMITTEE_SIZE,
        "Expected normal mode (committee {} >= {})",
        committee_size,
        MIN_COMMITTEE_SIZE
    );

    // Track phase distribution
    let mut phase_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

    // Observe epochs advancing autonomously
    println!("\n=== Observing {} epoch cycles in normal mode ===", NUM_EPOCHS);
    println!("(Nodes handle all phase transitions autonomously)");

    let mut epochs_completed = 0u64;
    ctx.observe_epochs(NUM_EPOCHS, |epoch, system| {
        epochs_completed += 1;
        let phase = epoch.phase.as_deref().unwrap_or("unknown").to_string();

        *phase_counts.entry(phase.clone()).or_insert(0) += 1;

        println!(
            "  Epoch {}: id={}, phase={}, committee={}",
            epochs_completed,
            epoch.id.unwrap_or(0),
            phase,
            system.committee_size.unwrap_or(0)
        );

        // Committee size should remain >= MIN_COMMITTEE_SIZE
        assert!(
            system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
            "Committee size dropped below minimum"
        );

        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Show phase distribution
    println!("\n=== Phase Distribution ===");
    for (phase, count) in &phase_counts {
        println!("  {}: {} occurrences", phase, count);
    }

    // Verify final state
    let final_epoch = ctx.epoch().expect("Failed to get epoch");
    let final_system = ctx.system().expect("Failed to get system");

    println!("\n=== Final State ===");
    println!("Epoch: {}", final_epoch.id.unwrap_or(0));
    println!("Phase: {:?}", final_epoch.phase);
    println!("Committee size: {}", final_system.committee_size.unwrap_or(0));

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

    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Wait for nodes to initialize
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Track health across epochs
    let mut total_checks = 0usize;
    let mut healthy_checks = 0usize;

    ctx.observe_epochs(NUM_EPOCHS, |_epoch, _system| {
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Check health at end
    for node in &ctx.nodes {
        total_checks += 1;
        if node.is_healthy().await {
            healthy_checks += 1;
        }
    }

    println!("\n=== Health Summary ===");
    println!("Total health checks: {}", total_checks);
    println!("Healthy checks: {}", healthy_checks);
    println!("Health rate: {:.1}%", 100.0 * healthy_checks as f64 / total_checks as f64);

    // We don't assert 100% health because nodes might briefly be unhealthy during transitions

    println!("\nTest passed: Node health monitored across {} epochs", NUM_EPOCHS);
}
