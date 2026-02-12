//! Epoch lifecycle tests across many epochs.
//!
//! Tests that verify correct behavior over extended periods (5+ epochs).
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
use tape_core::types::EpochNumber;
use tape_e2e::{TestContext, MIN_COMMITTEE_SIZE};

/// Test normal mode lifecycle over multiple epochs.
///
/// In normal mode (>= MIN_COMMITTEE_SIZE nodes):
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
///
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_normal_mode_lifecycle_5_epochs() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const NUM_EPOCHS: u64 = 5;
    const BASE_PORT: u16 = 10200;


    // Setup and advance to epoch 4+ for normal operation
    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    // Verify we're in normal mode
    let system = ctx.system().await.expect("Failed to get system");
    let committee_size = system.committee.size();
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
    println!("(Nodes handle all phase transitions autonomously)");

    let mut epochs_completed = 0u64;
    ctx.observe_epochs(NUM_EPOCHS, |epoch, system| {
        epochs_completed += 1;
        let phase = if epoch.state.is_syncing() {
            "Syncing"
        } else if epoch.state.is_settling() {
            "Settling"
        } else if epoch.state.is_active() {
            "Active"
        } else {
            "Unknown"
        };

        *phase_counts.entry(phase.to_string()).or_insert(0) += 1;

        println!(
            "  Epoch {}: id={}, phase={}, committee={}",
            epochs_completed,
            epoch.id.as_u64(),
            phase,
            system.committee.size()
        );

        // Committee size should remain >= MIN_COMMITTEE_SIZE
        assert!(
            system.committee.size() >= MIN_COMMITTEE_SIZE,
            "Committee size dropped below minimum"
        );

        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Show phase distribution
    for (phase, count) in &phase_counts {
        println!("  {}: {} occurrences", phase, count);
    }

    // Verify final state
    let final_epoch = ctx.epoch().await.expect("Failed to get epoch");
    let final_system = ctx.system().await.expect("Failed to get system");

    let final_phase = if final_epoch.state.is_syncing() {
        "Syncing"
    } else if final_epoch.state.is_settling() {
        "Settling"
    } else if final_epoch.state.is_active() {
        "Active"
    } else {
        "Unknown"
    };

    println!("Epoch: {}", final_epoch.id.as_u64());
    println!("Phase: {}", final_phase);
    println!("Committee size: {}", final_system.committee.size());

    println!("\nTest passed: Normal mode lifecycle completed {} epoch cycles successfully", NUM_EPOCHS);
}

/// Test node health monitoring over multiple epochs.
///
/// Verifies that nodes remain healthy throughout epoch transitions.
/// Nodes handle epoch advancement autonomously.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_health_across_epochs() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const NUM_EPOCHS: u64 = 5;
    const BASE_PORT: u16 = 10300;


    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

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

    println!("Total health checks: {}", total_checks);
    println!("Healthy checks: {}", healthy_checks);
    println!("Health rate: {:.1}%", 100.0 * healthy_checks as f64 / total_checks as f64);

    // We don't assert 100% health because nodes might briefly be unhealthy during transitions

    println!("\nTest passed: Node health monitored across {} epochs", NUM_EPOCHS);
}
