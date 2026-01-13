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

use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serial_test::serial;
use tape_e2e::{
    Tapedrive, TestNode, Validator, ValidatorOptions, wait_for_rpc,
    MIN_EPOCH_WAIT,
};

/// Wait for MIN_EPOCH_DURATION to pass before advancing epoch.
async fn wait_for_epoch_advance() {
    tokio::time::sleep(MIN_EPOCH_WAIT).await;
}

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

    println!("=== Concurrent AdvanceEpoch Spam Test ===");
    println!("Nodes: {}, Attackers: {}, Rounds: {}", NUM_NODES, NUM_ATTACKERS, SPAM_ROUNDS);

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

    // Register and start nodes
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register");
        node.stake(&cli, 1000).expect("Failed to stake");
        node.join(&cli).expect("Failed to join");
        nodes.push(node);
    }

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance");

    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let initial_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("Initial epoch: {}", initial_epoch.id.unwrap_or(0));

    // Spam AdvanceEpoch from multiple concurrent tasks
    println!("\n=== Starting spam attack ===");

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
        if let Ok(epoch) = cli.account_epoch() {
            println!(
                "  After round {}: epoch={}, phase={:?}",
                round + 1,
                epoch.id.unwrap_or(0),
                epoch.phase
            );
        }
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let total_failure = failure_count.load(Ordering::SeqCst);

    println!("\n=== Spam Results ===");
    println!("Total attempts: {}", total_success + total_failure);
    println!("Successes: {}", total_success);
    println!("Failures: {}", total_failure);

    // Most attempts should fail (epoch duration constraint)
    assert!(
        total_failure > total_success,
        "Most spam attempts should be rejected"
    );

    // Check nodes are still healthy
    println!("\n=== Checking node health ===");
    let mut all_healthy = true;
    for node in &nodes {
        let healthy = node.is_healthy().await;
        if !healthy {
            all_healthy = false;
            println!("  {} NOT healthy", node.name);
        }
    }

    assert!(all_healthy, "Nodes should remain healthy during spam");

    // Check for errors in logs
    let mut found_errors = false;
    for node in &nodes {
        if let Ok(log) = node.read_log() {
            if log.contains("panic") || log.contains("PANIC") {
                found_errors = true;
                println!("Error in {}", node.name);
            }
        }
    }

    assert!(!found_errors, "Nodes should not crash during spam attack");

    // Verify epoch advanced correctly (considering spam)
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("\nFinal epoch: {}", final_epoch.id.unwrap_or(0));

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

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

    println!("=== AdvancePool Spam Test ===");

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

    // Register and start nodes
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register");
        node.stake(&cli, 1000).expect("Failed to stake");
        node.join(&cli).expect("Failed to join");
        nodes.push(node);
    }

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance");

    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("Nodes started");

    // Spam AdvancePool for each node
    println!("\n=== Spamming AdvancePool ===");

    let mut pool_successes = 0;
    let mut pool_failures = 0;

    for round in 0..10 {
        // Call advance_pool on each node rapidly
        for node in &nodes {
            for _ in 0..5 {
                match node.advance_pool(&cli) {
                    Ok(_) => pool_successes += 1,
                    Err(_) => pool_failures += 1,
                }
            }
        }

        // Brief pause between rounds
        tokio::time::sleep(Duration::from_secs(1)).await;

        if round % 3 == 0 {
            println!("  Round {}: {} successes, {} failures", round + 1, pool_successes, pool_failures);
        }
    }

    println!("\nAdvancePool results:");
    println!("  Successes: {}", pool_successes);
    println!("  Failures: {}", pool_failures);

    // Check node health
    let mut all_healthy = true;
    for node in &nodes {
        if !node.is_healthy().await {
            all_healthy = false;
            println!("  {} NOT healthy", node.name);
        }
    }

    assert!(all_healthy, "Nodes should survive AdvancePool spam");

    // Advance a few epochs to verify normal operation continues
    println!("\n=== Verifying normal operation ===");
    for i in 1..=5 {
        wait_for_epoch_advance().await;
        if let Err(e) = cli.admin_advance_epoch() {
            println!("  Epoch {} advance: {}", i, e);
        }

        let epoch = cli.account_epoch().expect("Failed to get epoch");
        println!("  Epoch {}: id={}", i, epoch.id.unwrap_or(0));
    }

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
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
    const TEST_DURATION_SECS: u64 = 120;  // 2 minutes
    const BASE_PORT: u16 = 12300;

    println!("=== Interleaved Permissionless Calls Test ===");
    println!("Duration: {}s", TEST_DURATION_SECS);

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

    // Register and start nodes
    let mut nodes: Vec<TestNode> = Vec::new();
    for i in 0..NUM_NODES {
        let mut node = TestNode::new(i, BASE_PORT).expect("Failed to create node");
        node.register(&cli).expect("Failed to register");
        node.stake(&cli, 1000).expect("Failed to stake");
        node.join(&cli).expect("Failed to join");
        nodes.push(node);
    }

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance");

    for node in nodes.iter_mut() {
        let _ = node.start(&cli);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("Nodes started");

    // Counters for tracking
    let epoch_advances = Arc::new(AtomicU32::new(0));
    let pool_advances = Arc::new(AtomicU32::new(0));
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn background task that continuously calls permissionless functions
    let stop_clone = Arc::clone(&stop_flag);
    let epoch_clone = Arc::clone(&epoch_advances);
    let pool_clone = Arc::clone(&pool_advances);

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
    println!("\n=== Running for {}s with background spam ===", TEST_DURATION_SECS);

    let start = std::time::Instant::now();
    let mut last_epoch_id = 0u64;

    while start.elapsed().as_secs() < TEST_DURATION_SECS {
        // Do normal operations - check epoch, call advance_pool
        if let Ok(epoch) = cli.account_epoch() {
            let current_id = epoch.id.unwrap_or(0);
            if current_id != last_epoch_id {
                println!(
                    "  [{:3}s] Epoch changed: {} -> {}, phase: {:?}",
                    start.elapsed().as_secs(),
                    last_epoch_id,
                    current_id,
                    epoch.phase
                );
                last_epoch_id = current_id;
            }
        }

        // Call advance_pool on nodes
        for node in &nodes {
            if node.advance_pool(&cli).is_ok() {
                pool_advances.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Check node health periodically
        if start.elapsed().as_secs() % 30 == 0 {
            let mut healthy_count = 0;
            for node in &nodes {
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

    println!("\n=== Test Results ===");
    println!("Epoch advances: {}", epoch_advances.load(Ordering::Relaxed));
    println!("Pool advances: {}", pool_advances.load(Ordering::Relaxed));

    // Final health check
    println!("\n=== Final Health Check ===");
    let mut all_healthy = true;
    for node in &nodes {
        let healthy = node.is_healthy().await;
        println!("  {}: {}", node.name, if healthy { "healthy" } else { "NOT healthy" });
        if !healthy {
            all_healthy = false;
        }
    }

    // Check logs for errors
    println!("\n=== Checking Logs ===");
    let mut found_errors = false;
    for node in &nodes {
        if let Ok(log) = node.read_log() {
            let has_panic = log.contains("panic") || log.contains("PANIC");
            let has_bad_spool = log.contains("BadSpoolHash");
            let has_bad_epoch = log.contains("BadEpochId");

            if has_panic || has_bad_spool || has_bad_epoch {
                found_errors = true;
                println!("  {}: ERRORS FOUND", node.name);
                if has_panic { println!("    - panic"); }
                if has_bad_spool { println!("    - BadSpoolHash"); }
                if has_bad_epoch { println!("    - BadEpochId"); }
            } else {
                println!("  {}: no errors", node.name);
            }
        }
    }

    assert!(all_healthy, "All nodes should be healthy after test");
    assert!(!found_errors, "No errors should be found in logs");

    // Cleanup
    for node in nodes.iter_mut() {
        node.stop();
    }

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

    println!("=== Epoch Boundary Timing Test ===");

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

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance");
    let _ = node.start(&cli);

    println!("Node started");

    // Get current epoch timing
    let epoch = cli.account_epoch().expect("Failed to get epoch");
    let last_epoch = epoch.last_epoch.unwrap_or(0);
    println!("Last epoch timestamp: {}", last_epoch);

    // Run multiple epoch boundary tests
    println!("\n=== Testing epoch boundaries ===");

    for test_num in 1..=5 {
        println!("\nBoundary test {}:", test_num);

        // Wait until just before epoch can advance
        let target_wait = MIN_EPOCH_WAIT.as_secs() - 2;
        println!("  Waiting {}s (2s before boundary)...", target_wait);
        tokio::time::sleep(Duration::from_secs(target_wait)).await;

        // Try rapid advances right around the boundary
        let mut attempts = Vec::new();
        for _i in 0..20 {
            let result = cli.admin_advance_epoch().is_ok();
            attempts.push(result);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let successes: usize = attempts.iter().filter(|&&r| r).count();
        println!("  Attempts around boundary: {} successes out of {}", successes, attempts.len());

        // Should have exactly 1 success (at most)
        assert!(
            successes <= 1,
            "Should only advance once per epoch duration"
        );

        // Check node health
        assert!(node.is_healthy().await, "Node should remain healthy");
    }

    // Final verification
    let final_epoch = cli.account_epoch().expect("Failed to get epoch");
    println!("\nFinal epoch: {}", final_epoch.id.unwrap_or(0));

    // Check logs
    if let Ok(log) = node.read_log() {
        assert!(!log.contains("panic"), "Node should not panic");
    }

    node.stop();
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

    println!("=== Invalid State Calls Test ===");

    let validator = Validator::spawn_with_options(
        ValidatorOptions::default()
            .with_timeout(Duration::from_secs(120))
    )
    .await
    .expect("Failed to spawn validator");

    wait_for_rpc(validator.rpc_url(), Duration::from_secs(30))
        .await
        .expect("Validator did not become ready");

    let cli = Tapedrive::new_localnet();

    // Try operations before system initialization
    println!("\n=== Before System Init ===");

    // These should fail gracefully
    let pre_init_results = vec![
        ("admin_advance_epoch", cli.admin_advance_epoch().is_err()),
        ("account_system", cli.account_system().is_err()),
        ("account_epoch", cli.account_epoch().is_err()),
    ];

    for (name, failed) in &pre_init_results {
        println!("  {}: {}", name, if *failed { "failed (expected)" } else { "succeeded" });
    }

    // Now initialize
    cli.admin_init().expect("Failed to initialize system");
    println!("\nSystem initialized");

    // Try operations before any nodes
    println!("\n=== Before Any Nodes ===");

    // AdvanceEpoch with no committee
    let result = cli.admin_advance_epoch();
    println!("  advance_epoch (no nodes): {:?}", result.is_err());

    // Create a node but don't stake/join
    let mut node = TestNode::new(0, BASE_PORT).expect("Failed to create node");
    node.register(&cli).expect("Failed to register");

    // Try to start without joining committee
    // This might succeed but node won't participate
    println!("\n=== Node Without Committee Membership ===");
    let _ = node.start(&cli);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let healthy = node.is_healthy().await;
    println!("  Node healthy (not in committee): {}", healthy);

    // Now properly join
    node.stake(&cli, 1000).expect("Failed to stake");
    node.join(&cli).expect("Failed to join");

    wait_for_epoch_advance().await;
    cli.admin_advance_epoch().expect("Failed to advance");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let healthy = node.is_healthy().await;
    println!("  Node healthy (in committee): {}", healthy);

    // Try double-join (should fail gracefully)
    println!("\n=== Double Join Attempt ===");
    let double_join = node.join(&cli);
    println!("  Double join: {:?}", double_join.is_err());

    // Verify node is still healthy
    assert!(node.is_healthy().await, "Node should survive invalid state calls");

    // Check logs
    if let Ok(log) = node.read_log() {
        assert!(!log.contains("panic"), "Node should not panic from invalid calls");
    }

    node.stop();
    println!("\nTest passed: Invalid state calls handled gracefully");
}
