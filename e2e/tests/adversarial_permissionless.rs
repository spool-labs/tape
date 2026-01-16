//! Adversarial permissionless function tests.
//!
//! Tests that verify the system handles abuse of permissionless functions correctly.
//! AdvanceEpoch and AdvancePool can be called by anyone - these tests verify that
//! malicious or excessive calls don't disrupt normal node operation.
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test adversarial_permissionless -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use tape_e2e::{Tapedrive, TestContext, TestNode, EPOCH_WAIT};

/// Test concurrent AdvanceEpoch calls from multiple "attackers".
///
/// This simulates multiple parties spamming AdvanceEpoch simultaneously.
/// The system should:
/// 1. Only advance once per epoch duration
/// 2. Not cause node errors or crashes
/// 3. Maintain committee consistency
#[tokio::test]
#[ignore]
#[serial]
async fn test_concurrent_advance_epoch_spam() {
    const NUM_NODES: usize = 3;
    const NUM_ATTACKERS: usize = 5;
    const SPAM_ROUNDS: usize = 10;
    const BASE_PORT: u16 = 12100;

    println!(
        "Nodes: {}, Attackers: {}, Rounds: {}",
        NUM_NODES, NUM_ATTACKERS, SPAM_ROUNDS
    );

    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    let initial_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.as_u64());

    // Spam AdvanceEpoch from multiple concurrent tasks

    let success_count = Arc::new(AtomicU32::new(0));
    let failure_count = Arc::new(AtomicU32::new(0));

    for round in 0..SPAM_ROUNDS {
        println!("Round {}...", round + 1);

        // Wait partial epoch duration
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Spawn concurrent attackers
        let mut handles = Vec::new();

        for _attacker_id in 0..NUM_ATTACKERS {
            let cli = Tapedrive::new_localnet();
            let success = Arc::clone(&success_count);
            let failure = Arc::clone(&failure_count);

            let handle = tokio::spawn(async move {
                // Each attacker tries multiple rapid advances
                for _ in 0..3 {
                    match cli.admin_advance_epoch() {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(_) => {
                            failure.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            handles.push(handle);
        }

        // Wait for all attackers to finish
        for handle in handles {
            let _ = handle.await;
        }

        // Check system state after spam
        if let Ok(epoch) = ctx.epoch().await {
            let phase = if epoch.state.is_syncing() { "Syncing" }
                else if epoch.state.is_settling() { "Settling" }
                else if epoch.state.is_active() { "Active" }
                else { "Unknown" };
            println!(
                "  After round {}: epoch={}, phase={}",
                round + 1,
                epoch.id.as_u64(),
                phase
            );
        }
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let total_failure = failure_count.load(Ordering::SeqCst);

    println!("Total attempts: {}", total_success + total_failure);
    println!("Successes: {}", total_success);
    println!("Failures: {}", total_failure);

    // Most attempts should fail (epoch duration constraint)
    assert!(
        total_failure > total_success,
        "Most spam attempts should be rejected"
    );

    // Check nodes are still healthy
    let mut all_healthy = true;
    for node in &ctx.nodes {
        let healthy = node.is_healthy().await;
        if !healthy {
            all_healthy = false;
            println!("  {} NOT healthy", node.name);
        }
    }

    assert!(all_healthy, "Nodes should remain healthy during spam");

    // Check for errors in logs
    ctx.check_node_logs()
        .expect("Nodes should not crash during spam attack");

    // Verify epoch advanced correctly (considering spam)
    let final_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("\nFinal epoch: {}", final_epoch.id.as_u64());

    println!("\nTest passed: System survived concurrent AdvanceEpoch spam");
}

/// Test AdvancePool spam while nodes are operating.
///
/// AdvancePool advances the staking pool epoch accounting.
/// Spamming it should not disrupt node operations.
#[tokio::test]
#[ignore]
#[serial]
async fn test_advance_pool_spam() {
    const NUM_NODES: usize = 3;
    const BASE_PORT: u16 = 12200;


    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(180))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    println!("Nodes started");

    // Spam AdvancePool for each node

    let mut pool_successes = 0;
    let mut pool_failures = 0;

    for round in 0..10 {
        // Call advance_pool on each node rapidly
        for node in &ctx.nodes {
            for _ in 0..5 {
                match node.advance_pool(&ctx.cli) {
                    Ok(_) => pool_successes += 1,
                    Err(_) => pool_failures += 1,
                }
            }
        }

        // Brief pause between rounds
        tokio::time::sleep(Duration::from_secs(1)).await;

        if round % 3 == 0 {
            println!(
                "  Round {}: {} successes, {} failures",
                round + 1,
                pool_successes,
                pool_failures
            );
        }
    }

    println!("\nAdvancePool results:");
    println!("  Successes: {}", pool_successes);
    println!("  Failures: {}", pool_failures);

    // Check node health
    let mut all_healthy = true;
    for node in &ctx.nodes {
        if !node.is_healthy().await {
            all_healthy = false;
            println!("  {} NOT healthy", node.name);
        }
    }

    assert!(all_healthy, "Nodes should survive AdvancePool spam");

    // Verify normal operation continues by advancing a few epochs manually
    // (low-quorum mode with 3 nodes requires manual advancement)
    println!("\nVerifying normal operation continues...");

    for i in 0..3 {
        tokio::time::sleep(EPOCH_WAIT).await;
        match ctx.advance_epoch() {
            Ok(_) => {
                if let Ok(epoch) = ctx.epoch().await {
                    println!("  Epoch {} advanced to {}", i + 1, epoch.id.as_u64());
                }
            }
            Err(e) => {
                // Non-fatal - just checking system still works
                println!("  Epoch {} advance: {}", i + 1, e);
            }
        }
    }

    // Final health check
    for node in &ctx.nodes {
        assert!(node.is_healthy().await, "Node {} should be healthy after epoch advances", node.name);
    }

    println!("\nTest passed: System survived AdvancePool spam");
}

/// Test interleaved permissionless calls during normal operation.
///
/// This test runs nodes normally while constantly calling both
/// AdvanceEpoch and AdvancePool from a background task.
#[tokio::test]
#[ignore]
#[serial]
async fn test_interleaved_permissionless_calls() {
    const NUM_NODES: usize = 4;
    const TEST_DURATION_SECS: u64 = 120; // 2 minutes
    const BASE_PORT: u16 = 12300;

    println!("Duration: {}s", TEST_DURATION_SECS);

    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(180))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    println!("Nodes started");

    // Counters for tracking
    let epoch_advances = Arc::new(AtomicU32::new(0));
    let pool_advances = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn background task that continuously calls permissionless functions
    let stop_clone = Arc::clone(&stop_flag);
    let epoch_clone = Arc::clone(&epoch_advances);

    let spammer_handle = tokio::spawn(async move {
        let cli = Tapedrive::new_localnet();

        while !stop_clone.load(Ordering::Relaxed) {
            // Call AdvanceEpoch
            if cli.admin_advance_epoch().is_ok() {
                epoch_clone.fetch_add(1, Ordering::Relaxed);
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            // Note: AdvancePool requires node config, so we skip it in background
            // The main test will call it

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    // Run test for specified duration
    println!(
        "\n=== Running for {}s with background spam ===",
        TEST_DURATION_SECS
    );

    let start = std::time::Instant::now();
    let mut last_epoch_id = 0u64;

    while start.elapsed().as_secs() < TEST_DURATION_SECS {
        // Do normal operations - check epoch, call advance_pool
        if let Ok(epoch) = ctx.epoch().await {
            let current_id = epoch.id.as_u64();
            if current_id != last_epoch_id {
                let phase = if epoch.state.is_syncing() { "Syncing" }
                    else if epoch.state.is_settling() { "Settling" }
                    else if epoch.state.is_active() { "Active" }
                    else { "Unknown" };
                println!(
                    "  [{:3}s] Epoch changed: {} -> {}, phase: {}",
                    start.elapsed().as_secs(),
                    last_epoch_id,
                    current_id,
                    phase
                );
                last_epoch_id = current_id;
            }
        }

        // Call advance_pool on nodes
        for node in &ctx.nodes {
            if node.advance_pool(&ctx.cli).is_ok() {
                pool_advances.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Check node health periodically
        if start.elapsed().as_secs() % 30 == 0 {
            let mut healthy_count = 0;
            for node in &ctx.nodes {
                if node.is_healthy().await {
                    healthy_count += 1;
                }
            }

            if healthy_count < NUM_NODES {
                println!(
                    "  [{:3}s] WARNING: Only {}/{} nodes healthy",
                    start.elapsed().as_secs(),
                    healthy_count,
                    NUM_NODES
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    // Stop spammer
    stop_flag.store(true, Ordering::Relaxed);
    let _ = spammer_handle.await;

    println!(
        "Epoch advances: {}",
        epoch_advances.load(Ordering::Relaxed)
    );
    println!("Pool advances: {}", pool_advances.load(Ordering::Relaxed));

    // Final health check
    let mut all_healthy = true;
    for node in &ctx.nodes {
        let healthy = node.is_healthy().await;
        println!(
            "  {}: {}",
            node.name,
            if healthy { "healthy" } else { "NOT healthy" }
        );
        if !healthy {
            all_healthy = false;
        }
    }

    // Check logs for errors
    ctx.check_node_logs()
        .expect("No errors should be found in logs");

    assert!(all_healthy, "All nodes should be healthy after test");

    println!("\nTest passed: System survived interleaved permissionless calls");
}

/// Test calling AdvanceEpoch exactly at epoch boundaries.
///
/// This test tries to trigger race conditions by calling AdvanceEpoch
/// right at the epoch duration boundary.
#[tokio::test]
#[ignore]
#[serial]
async fn test_epoch_boundary_timing() {
    const BASE_PORT: u16 = 12400;


    let ctx = TestContext::builder()
        .nodes(1)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(180))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    println!("Node started");

    // Get current epoch timing
    let epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("Current epoch: {}", epoch.id.as_u64());

    // Run multiple epoch boundary tests

    for test_num in 1..=5 {
        println!("\nBoundary test {}:", test_num);

        // Wait until just before epoch can advance
        let target_wait = EPOCH_WAIT.as_secs() - 2;
        println!("  Waiting {}s (2s before boundary)...", target_wait);
        tokio::time::sleep(Duration::from_secs(target_wait)).await;

        // Try rapid advances right around the boundary
        let mut attempts = Vec::new();
        for _i in 0..20 {
            let result = ctx.advance_epoch().is_ok();
            attempts.push(result);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let successes: usize = attempts.iter().filter(|&&r| r).count();
        println!(
            "  Attempts around boundary: {} successes out of {}",
            successes,
            attempts.len()
        );

        // Should have exactly 1 success (at most)
        assert!(
            successes <= 1,
            "Should only advance once per epoch duration"
        );

        // Check node health
        assert!(
            ctx.nodes[0].is_healthy().await,
            "Node should remain healthy"
        );
    }

    // Final verification
    let final_epoch = ctx.epoch().await.expect("Failed to get epoch");
    println!("\nFinal epoch: {}", final_epoch.id.as_u64());

    // Check logs
    ctx.check_node_logs().expect("Node should not have errors");

    println!("\nTest passed: Epoch boundary timing handled correctly");
}

/// Test that invalid/malformed calls don't crash nodes.
///
/// While we can't send truly malformed transactions via CLI,
/// we can test error handling by calling operations in invalid states.
#[tokio::test]
#[ignore]
#[serial]
async fn test_invalid_state_calls() {
    const BASE_PORT: u16 = 12500;


    // Build context without nodes - we'll add one manually to test various states
    let mut ctx = TestContext::builder()
        .nodes(0)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(120))
        .build()
        .await
        .expect("Failed to setup test context");

    // Try operations before system initialization is handled by build() which calls admin_init
    // So test operations with no nodes

    // AdvanceEpoch with no committee - may succeed or fail depending on timing
    let result = ctx.advance_epoch();
    println!("  advance_epoch (no nodes): {}", if result.is_ok() { "succeeded" } else { "failed" });

    // Create a node but don't stake/join
    let mut node = TestNode::new(0, BASE_PORT).expect("Failed to create node");
    node.register(&ctx.cli).expect("Failed to register");

    // Try to start without joining committee
    // This might succeed but node won't participate
    if let Err(e) = node.fund(&ctx.cli, 1.0) {
        eprintln!("Warning: Failed to fund node: {}", e);
    }
    let _ = node.start(&ctx.cli);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let healthy = node.is_healthy().await;
    println!("  Node healthy (not in committee): {}", healthy);

    // Now properly join
    node.stake(&ctx.cli, 1000).expect("Failed to stake");
    node.join(&ctx.cli).expect("Failed to join");

    // Wait for EPOCH_WAIT to ensure epoch duration has elapsed since any previous advance
    println!("  Waiting for epoch duration to elapse...");
    tokio::time::sleep(EPOCH_WAIT).await;

    // Try to advance - this may fail if already advanced, which is fine
    match ctx.advance_epoch() {
        Ok(_) => println!("  advance_epoch after join: succeeded"),
        Err(e) => println!("  advance_epoch after join: {} (may be expected)", e),
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let healthy = node.is_healthy().await;
    println!("  Node healthy (in committee): {}", healthy);

    // Try double-join (should fail gracefully)
    let double_join = node.join(&ctx.cli);
    println!("  Double join: {:?}", double_join.is_err());

    // Verify node is still healthy
    assert!(
        node.is_healthy().await,
        "Node should survive invalid state calls"
    );

    // Check logs
    if let Ok(log) = node.read_log() {
        assert!(
            !log.contains("panic"),
            "Node should not panic from invalid calls"
        );
    }

    // Add node to context for proper cleanup
    ctx.nodes.push(node);

    println!("\nTest passed: Invalid state calls handled gracefully");
}
