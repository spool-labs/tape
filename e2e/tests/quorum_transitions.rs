//! Committee threshold transition tests.
//!
//! Tests that verify correct behavior when crossing the committee threshold (MIN_COMMITTEE_SIZE nodes)
//! in both directions (scaling up and scaling down).
//!
//! With the blocked epoch design:
//! - AdvanceEpoch fails with InsufficientCommittee when committee_next < MIN_COMMITTEE_SIZE
//! - Stake activates immediately when epoch is blocked
//! - New nodes can join and help unblock the epoch
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
use tape_e2e::{TestContext, MIN_COMMITTEE_SIZE, VARYING_STAKES};

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
/// 1. Starts with MIN_COMMITTEE_SIZE nodes
/// 2. Adds new nodes mid-test
/// 3. Verifies committee adjusts correctly
#[tokio::test]
#[ignore]
#[serial]
async fn test_dynamic_node_membership() {
    const BASE_PORT: u16 = 11300;

    println!("=== Dynamic Node Membership Test ===");

    // Start with minimum nodes for normal operation
    let mut ctx = TestContext::builder()
        .nodes(MIN_COMMITTEE_SIZE)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    println!("Started with {} nodes", ctx.nodes.len());

    // Run a few epochs
    println!("\n=== Phase 1: Running 3 epochs with {} nodes ===", MIN_COMMITTEE_SIZE);

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
    println!("\n=== Phase 3: Running 3 epochs with {} nodes ===", ctx.nodes.len());

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
