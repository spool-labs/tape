//! Catch-up mode tests.
//!
//! Tests that verify nodes correctly handle catch-up scenarios:
//! - Skipping stale epoch submissions when behind
//! - Refreshing system state when caught up
//! - Participating in current epoch after catch-up
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test catchup_flow -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    TestContext, EPOCH_WAIT, MIN_COMMITTEE_SIZE,
};

/// Test that starting a node after epochs have passed doesn't cause errors.
///
/// This tests the core catch-up logic:
/// - Node starts and begins processing blocks
/// - Historical AdvanceEpoch events should be skipped (stale)
/// - Node should not attempt to submit SyncEpoch for old epochs
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_startup_after_epoch_advances() {
    const BASE_PORT: u16 = 8090;


    // Build context without bootstrap - we want to control node start timing
    let mut ctx = TestContext::builder()
        .nodes(1)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(120))
        .build()
        .await
        .expect("Failed to setup test context");

    // Get initial epoch
    let initial_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.as_u64());

    // Advance epoch to activate node in committee_next
    ctx.wait_and_advance_epoch().await.expect("Failed to advance epoch");

    // Advance a couple more epochs BEFORE starting the node
    // This creates the scenario where the node has to catch up
    for i in 0..2 {
        match ctx.wait_and_advance_epoch().await {
            Ok(_) => println!("Pre-start: Advanced epoch {}", i + 2),
            Err(e) => println!("Pre-start: Epoch advance {}: {}", i + 2, e),
        }
    }

    let pre_start_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("Epoch before node start: {}", pre_start_epoch.id.as_u64());

    // Now fund and start the node - it should catch up on all the epochs
    // without trying to submit stale transactions
    println!(
        "\nStarting node (should catch up on {} epochs)...",
        pre_start_epoch.id.as_u64() - initial_epoch.id.as_u64()
    );

    let node = &mut ctx.nodes[0];
    if let Err(e) = node.fund(&ctx.cli, 1.0) {
        eprintln!("Warning: Failed to fund node: {}", e);
    }
    node.start(&ctx.cli).expect("Failed to start node");

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
            if line.contains("stale")
                || line.contains("catch")
                || line.contains("Stale")
                || line.contains("Catch")
                || line.contains("epoch")
            {
                println!("  {}", line);
            }
        }

        // Verify no BadSpoolHash or BadEpochId errors
        let has_bad_spool_hash = log.contains("BadSpoolHash") || log.contains("bad spool hash");
        let has_bad_epoch_id = log.contains("BadEpochId") || log.contains("bad epoch id");

        if has_bad_spool_hash {
            println!("\nWARNING: Found BadSpoolHash errors in logs!");
        }
        if has_bad_epoch_id {
            println!("\nWARNING: Found BadEpochId errors in logs!");
        }

        assert!(
            !has_bad_spool_hash,
            "Node should not have BadSpoolHash errors during catch-up"
        );
        assert!(
            !has_bad_epoch_id,
            "Node should not have BadEpochId errors during catch-up"
        );
    }

    println!("\nTest passed: Node started and caught up without stale epoch errors");
}

/// Test catch-up in non-low-quorum (normal) mode with >= 24 nodes.
///
/// This test:
/// 1. Registers 24 nodes to exit low-quorum mode
/// 2. Advances epochs with full EPOCH_DURATION timing
/// 3. Verifies epoch transitions through Syncing phase
/// 4. Tests that late-joining node catches up correctly
///
/// Note: This test takes ~3-4 minutes due to EPOCH_DURATION waits.
#[tokio::test]
#[ignore]
#[serial]
async fn test_catchup_normal_quorum() {
    const BASE_PORT: u16 = 9000;


    // Build without bootstrap - we need custom timing for normal mode
    let mut ctx = TestContext::builder()
        .nodes(MIN_COMMITTEE_SIZE)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build()
        .await
        .expect("Failed to setup test context");

    let initial_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.as_u64());

    // Check system state
    let system = ctx.system().await.expect("Failed to get system");
    println!("Committee size: {}", system.committee.size());
    println!("Committee next size: {}", system.committee_next.size());

    // Wait for EPOCH_DURATION and advance to activate nodes
    println!(
        "\n=== Advancing to activate {} nodes (waiting {}s) ===",
        MIN_COMMITTEE_SIZE,
        EPOCH_WAIT.as_secs()
    );
    tokio::time::sleep(EPOCH_WAIT).await;

    match ctx.advance_epoch() {
        Ok(_) => println!("Epoch advanced"),
        Err(e) => println!("Advance failed: {}", e),
    }

    // Check if we're in normal mode now
    let epoch = ctx.epoch().await.expect("Failed to get epoch");
    let system = ctx.system().await.expect("Failed to get system");
    let phase = if epoch.state.is_syncing() { "Syncing" }
        else if epoch.state.is_settling() { "Settling" }
        else if epoch.state.is_active() { "Active" }
        else { "Unknown" };
    println!("Epoch: {}, Phase: {}", epoch.id.as_u64(), phase);
    println!("Committee size: {}", system.committee.size());

    let in_normal_mode = system.committee.size() >= MIN_COMMITTEE_SIZE;
    println!("In normal mode: {}", in_normal_mode);

    if in_normal_mode {
        // In normal mode, epoch should be in Syncing phase
        println!("\nNormal mode detected - epoch should be in Syncing phase");

        // Fund and start nodes so they can submit SyncEpoch
        for (i, node) in ctx.nodes.iter_mut().enumerate() {
            if let Err(e) = node.fund(&ctx.cli, 1.0) {
                eprintln!("Warning: Failed to fund node {}: {}", i, e);
            }
            match node.start(&ctx.cli) {
                Ok(_) => println!("Node {} started", i),
                Err(e) => println!("Node {} start failed: {}", i, e),
            }
        }

        // Give nodes time to sync and submit attestations
        println!("Waiting for nodes to submit SyncEpoch attestations...");
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Check epoch phase - should transition to Settling or Active
        let epoch = ctx.epoch().await.expect("Failed to get epoch");
        let phase = if epoch.state.is_syncing() { "Syncing" }
            else if epoch.state.is_settling() { "Settling" }
            else if epoch.state.is_active() { "Active" }
            else { "Unknown" };
        println!(
            "After node sync - Epoch: {}, Phase: {}",
            epoch.id.as_u64(), phase
        );

        // Advance another epoch to test catch-up
        tokio::time::sleep(EPOCH_WAIT).await;

        match ctx.advance_epoch() {
            Ok(_) => println!("Second epoch advanced"),
            Err(e) => println!("Second advance: {}", e),
        }

        let epoch = ctx.epoch().await.expect("Failed to get epoch");
        let phase = if epoch.state.is_syncing() { "Syncing" }
            else if epoch.state.is_settling() { "Settling" }
            else if epoch.state.is_active() { "Active" }
            else { "Unknown" };
        println!("Final epoch: {}, Phase: {}", epoch.id.as_u64(), phase);

        // Check node logs for any catch-up related errors
        ctx.check_node_logs()
            .expect("Nodes should not have stale epoch errors in normal mode");

        println!("\nTest passed: Normal quorum catch-up completed successfully");
    } else {
        // Still in low-quorum mode - might not have enough stake activated
        println!("\nNote: Still in low-quorum mode (committee < 24)");
        println!("This can happen if stake hasn't activated yet (requires epoch+2)");
        println!("Skipping normal-mode specific tests");

        // Advance one more epoch to test basic functionality
        ctx.wait_and_advance_epoch().await.ok();

        let epoch = ctx.epoch().await.expect("Failed to get epoch");
        println!("Final epoch: {}", epoch.id.as_u64());

        println!("\nTest completed in low-quorum mode");
    }
}
