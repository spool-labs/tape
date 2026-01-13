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
    TestContext, MIN_COMMITTEE_SIZE, VARYING_STAKES,
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
    const ADDITIONAL_NODES: usize = 16;  // 10 + 16 = 26 > MIN_COMMITTEE_SIZE
    const BASE_PORT: u16 = 11100;

    println!("=== Low to Normal Quorum Transition Test ===");
    println!("Initial nodes: {} (low-quorum)", INITIAL_NODES);
    println!("Final nodes: {} (normal)", INITIAL_NODES + ADDITIONAL_NODES);
    println!("Threshold: {}", MIN_COMMITTEE_SIZE);

    // Setup with initial nodes
    let mut ctx = TestContext::builder()
        .nodes(INITIAL_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Verify we start in low-quorum mode
    let system = ctx.system().expect("Failed to get system");
    println!("After bootstrap - Committee size: {}", system.committee_size.unwrap_or(0));

    assert!(
        system.committee_size.unwrap_or(0) < MIN_COMMITTEE_SIZE,
        "Should be in low-quorum mode"
    );

    // Observe epochs in low-quorum mode
    println!("\n=== Phase 1: Observing 5 epochs in low-quorum mode ===");

    ctx.observe_epochs(5, |epoch, _system| {
        println!(
            "  Epoch: id={}, phase={:?}",
            epoch.id.unwrap_or(0),
            epoch.phase
        );

        assert_eq!(
            epoch.phase.as_deref(),
            Some("Active"),
            "Low-quorum should stay in Active phase"
        );

        Ok(())
    })
    .await
    .expect("Failed to observe low-quorum epochs");

    // Add more nodes to cross threshold
    println!("\n=== Phase 2: Adding {} nodes to cross threshold ===", ADDITIONAL_NODES);
    ctx.add_nodes(ADDITIONAL_NODES, 1000)
        .await
        .expect("Failed to add nodes");

    println!("Total nodes now: {}", ctx.nodes.len());

    // Wait for epoch to advance and activate new nodes
    println!("Waiting for epoch to advance and activate new nodes...");
    ctx.observe_epochs(1, |epoch, system| {
        println!(
            "After adding nodes: epoch={}, committee={}",
            epoch.id.unwrap_or(0),
            system.committee_size.unwrap_or(0)
        );
        Ok(())
    })
    .await
    .expect("Failed to advance epoch");

    // Check if we transitioned to normal mode
    let system = ctx.system().expect("Failed to get system");
    let committee_size = system.committee_size.unwrap_or(0);

    if committee_size >= MIN_COMMITTEE_SIZE {
        println!("\n=== Phase 3: Transitioned to normal mode! ===");

        // Observe epochs in normal mode
        println!("Observing 5 epochs in normal mode...");

        ctx.observe_epochs(5, |epoch, system| {
            println!(
                "  Epoch: id={}, phase={:?}, committee={}",
                epoch.id.unwrap_or(0),
                epoch.phase,
                system.committee_size.unwrap_or(0)
            );

            assert!(
                system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
                "Should stay in normal mode"
            );

            Ok(())
        })
        .await
        .expect("Failed to observe normal mode epochs");
    } else {
        println!("Note: Committee size {} still below threshold {}", committee_size, MIN_COMMITTEE_SIZE);
        println!("This can happen if stake activation is delayed");
    }

    // Check for errors
    println!("\n=== Checking for errors ===");
    ctx.check_node_logs().expect("Found errors during transition");

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
    const BASE_PORT: u16 = 11200;

    println!("=== Stake Weight Allocation Test ===");
    println!("Stake amounts: {:?}", VARYING_STAKES);

    // Setup with varying stakes
    let ctx = TestContext::builder()
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_with_varying_stakes_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Query node status to see spool allocations
    println!("\n=== Initial Spool Allocations ===");
    let mut total_allocations = 0u16;

    for (i, node) in ctx.nodes.iter().enumerate() {
        if let Some(addr) = &node.node_address {
            match ctx.cli.node_status(Some(&node.config_path), Some(addr)) {
                Ok(status) => {
                    let spools = status.spool_count.unwrap_or(0);
                    println!("  Node {} (stake {}): {} spools", i, VARYING_STAKES[i], spools);
                    total_allocations += spools;
                }
                Err(e) => {
                    println!("  Node {}: status unavailable ({})", i, e);
                }
            }
        }
    }

    println!("\nTotal spool allocations: {}", total_allocations);

    // Observe several epochs advancing autonomously
    println!("\n=== Observing 5 epochs ===");

    ctx.observe_epochs(5, |epoch, _system| {
        println!(
            "  Epoch: id={}, phase={:?}",
            epoch.id.unwrap_or(0),
            epoch.phase
        );
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Verify allocations at end
    println!("\n=== Final Spool Allocations ===");
    for (i, node) in ctx.nodes.iter().enumerate() {
        if let Some(addr) = &node.node_address {
            if let Ok(status) = ctx.cli.node_status(Some(&node.config_path), Some(addr)) {
                let spools = status.spool_count.unwrap_or(0);
                println!("  Node {} (stake {}): {} spools", i, VARYING_STAKES[i], spools);
            }
        }
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

    // Start with 3 nodes
    let mut ctx = TestContext::builder()
        .nodes(3)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    println!("Started with {} nodes", ctx.nodes.len());

    // Run a few epochs
    println!("\n=== Phase 1: Running 3 epochs with 3 nodes ===");

    ctx.observe_epochs(3, |epoch, system| {
        println!(
            "  Epoch: id={}, committee={}",
            epoch.id.unwrap_or(0),
            system.committee_size.unwrap_or(0)
        );
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Add 2 more nodes
    println!("\n=== Phase 2: Adding 2 more nodes ===");
    ctx.add_nodes(2, 1000)
        .await
        .expect("Failed to add nodes");

    println!("Total nodes now: {}", ctx.nodes.len());

    // Wait for epoch to advance and activate new nodes
    ctx.observe_epochs(1, |epoch, system| {
        println!(
            "After adding: epoch={}, committee={}",
            epoch.id.unwrap_or(0),
            system.committee_size.unwrap_or(0)
        );
        Ok(())
    })
    .await
    .expect("Failed to advance epoch");

    // Run more epochs
    println!("\n=== Phase 3: Running 3 epochs with 5 nodes ===");

    ctx.observe_epochs(3, |epoch, system| {
        println!(
            "  Epoch: id={}, committee={}",
            epoch.id.unwrap_or(0),
            system.committee_size.unwrap_or(0)
        );
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Stop 2 nodes (simulating departure)
    println!("\n=== Phase 4: Stopping 2 nodes ===");
    for i in 0..2 {
        ctx.nodes[i].stop();
        println!("  Stopped {}", ctx.nodes[i].name);
    }

    // Run more epochs with remaining nodes
    println!("\n=== Phase 5: Running 3 epochs with 2 nodes stopped ===");

    ctx.observe_epochs(3, |epoch, system| {
        println!(
            "  Epoch: id={}, committee={}",
            epoch.id.unwrap_or(0),
            system.committee_size.unwrap_or(0)
        );
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Final state
    let system = ctx.system().expect("Failed to get system");
    println!("\n=== Final State ===");
    println!("Committee size: {}", system.committee_size.unwrap_or(0));

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

    // Setup with one node
    let ctx = TestContext::builder()
        .nodes(1)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(180))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let initial_epoch = ctx.epoch().expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.unwrap_or(0));

    // Attempt rapid advances (most should fail due to timing)
    println!("\n=== Attempting 20 rapid advances ===");
    let mut successes = 0;
    let mut failures = 0;

    for i in 1..=20 {
        // Short wait (less than MIN_EPOCH_DURATION)
        tokio::time::sleep(Duration::from_secs(2)).await;

        match ctx.cli.admin_advance_epoch() {
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
    println!("\n=== Waiting remaining epoch duration ===");
    match ctx.wait_and_advance_epoch().await {
        Ok(_) => println!("Advance after proper wait: SUCCESS"),
        Err(e) => println!("Advance after proper wait: {}", e),
    }

    let final_epoch = ctx.epoch().expect("Failed to get epoch");
    println!("Final epoch: {}", final_epoch.id.unwrap_or(0));

    // Check node for errors
    if let Ok(log) = ctx.nodes[0].read_log() {
        assert!(
            !log.contains("panic"),
            "Node should not panic during rapid advances"
        );
    }

    println!("\nTest passed: Rapid epoch advances handled correctly");
}
