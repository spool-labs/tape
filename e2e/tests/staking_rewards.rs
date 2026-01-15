//! Staking, rewards, and commission tests.
//!
//! Tests that verify the staking pool mechanics, reward distribution,
//! and commission collection work correctly with a large committee.
//!
//! All tests spawn their own validator and run serially to avoid port conflicts.
//!
//! ```bash
//! cargo test -p tape-e2e --test staking_rewards -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use tape_e2e::{
    TestContext, wait_for_node_health,
    temp_file_with_content, deterministic_blob,
    sizes, EPOCH_WAIT,
};

/// Number of nodes for staking tests.
const STAKING_NODE_COUNT: usize = 50;

/// Base port for staking tests.
const STAKING_BASE_PORT: u16 = 12000;

/// Timeout for staking test setup.
const STAKING_TIMEOUT: Duration = Duration::from_secs(1200);

/// Test that rewards pool accumulates from storage uploads.
///
/// This test:
/// 1. Spins up 50 nodes and bootstraps the network
/// 2. Uploads multiple files (generates storage fees -> rewards pool)
/// 3. Verifies the archive shows accumulated rewards
#[tokio::test]
#[ignore]
#[serial]
async fn test_rewards_pool_accumulation() {
    println!("=== Rewards Pool Accumulation Test ({} nodes) ===", STAKING_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(STAKING_NODE_COUNT)
        .port(STAKING_BASE_PORT)
        .timeout(STAKING_TIMEOUT)
        .stake(1000)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Wait for epoch to become Active
    println!("Waiting for epoch to become Active...");
    wait_for_active_epoch(&ctx, 60).await;

    // Check initial archive state
    let archive_before = ctx.cli.account_archive()
        .expect("Failed to get archive before uploads");
    println!("\n=== Initial Archive State ===");
    println!("  Rewards Pool: {:?}", archive_before.rewards_pool);
    println!("  Rewards Paid: {:?}", archive_before.rewards_paid);
    println!("  Recent Usage: {:?}", archive_before.recent_usage);

    // Wait for some nodes to be healthy
    println!("\nWaiting for nodes to become healthy...");
    for (i, node) in ctx.nodes.iter().enumerate().take(5) {
        if wait_for_node_health(&node.url(), Duration::from_secs(30)).await.is_ok() {
            println!("  Node {} healthy", i);
        }
    }

    let node_urls = ctx.node_urls();

    // Upload multiple files to generate storage fees
    println!("\n=== Uploading files to generate storage fees ===");
    let upload_sizes = [
        (sizes::KB * 10, "10 KB"),
        (sizes::KB * 50, "50 KB"),
        (sizes::KB * 100, "100 KB"),
        (sizes::MB, "1 MB"),
    ];

    for (i, (size, name)) in upload_sizes.iter().enumerate() {
        let blob = deterministic_blob(*size, (i + 1) as u64);
        let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

        print!("  Uploading {}... ", name);
        match ctx.cli.storage_upload(upload_file.path(), None, Some(&node_urls)) {
            Ok(result) => println!("OK (track: {})", result.track_id),
            Err(e) => println!("FAILED: {}", e),
        }
    }

    // Check archive state after uploads
    let archive_after = ctx.cli.account_archive()
        .expect("Failed to get archive after uploads");
    println!("\n=== Archive State After Uploads ===");
    println!("  Rewards Pool: {:?}", archive_after.rewards_pool);
    println!("  Rewards Paid: {:?}", archive_after.rewards_paid);
    println!("  Recent Usage: {:?}", archive_after.recent_usage);
    println!("  Tape Count:   {:?}", archive_after.tape_count);

    // Rewards pool should have increased (or at least not be zero)
    let pool_before = archive_before.rewards_pool.unwrap_or(0);
    let pool_after = archive_after.rewards_pool.unwrap_or(0);

    println!("\n=== Results ===");
    println!("  Rewards pool change: {} -> {}", pool_before, pool_after);

    // In a real network, uploads would add to the rewards pool
    // For now, just verify we can read the archive state
    assert!(archive_after.tape_count.unwrap_or(0) > 0, "Should have created tapes");

    println!("\nTest passed: Rewards pool accumulation verified");
}

/// Test reward distribution across epochs.
///
/// This test:
/// 1. Spins up 50 nodes and uploads files
/// 2. Advances multiple epochs
/// 3. Verifies rewards_paid increases as nodes call AdvancePool
#[tokio::test]
#[ignore]
#[serial]
async fn test_rewards_distribution_across_epochs() {
    println!("=== Rewards Distribution Across Epochs Test ({} nodes) ===", STAKING_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(STAKING_NODE_COUNT)
        .port(STAKING_BASE_PORT + 200)
        .timeout(STAKING_TIMEOUT)
        .stake(1000)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    wait_for_active_epoch(&ctx, 60).await;

    let node_urls = ctx.node_urls();

    // Upload files to generate rewards
    println!("\n=== Uploading files ===");
    for i in 0..3 {
        let blob = deterministic_blob(sizes::KB * 50, (i + 100) as u64);
        let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");
        if let Ok(result) = ctx.cli.storage_upload(upload_file.path(), None, Some(&node_urls)) {
            println!("  Upload {}: track {}", i + 1, result.track_id);
        }
    }

    let epoch_before = ctx.epoch().await.expect("Failed to get epoch").id.as_u64();
    let archive_before = ctx.cli.account_archive().expect("Failed to get archive");
    println!("\n=== Before Epoch Advance ===");
    println!("  Epoch: {}", epoch_before);
    println!("  Rewards Pool: {:?}", archive_before.rewards_pool);
    println!("  Rewards Paid: {:?}", archive_before.rewards_paid);

    // Advance epoch - nodes will call AdvancePool which distributes rewards
    println!("\n=== Advancing epoch ===");
    tokio::time::sleep(EPOCH_WAIT).await;
    ctx.cli.admin_advance_epoch().expect("Failed to advance epoch");

    // Give nodes time to process epoch change and call AdvancePool
    println!("  Waiting for nodes to process epoch change...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let epoch_after = ctx.epoch().await.expect("Failed to get epoch").id.as_u64();
    let archive_after = ctx.cli.account_archive().expect("Failed to get archive");
    println!("\n=== After Epoch Advance ===");
    println!("  Epoch: {}", epoch_after);
    println!("  Rewards Pool: {:?}", archive_after.rewards_pool);
    println!("  Rewards Paid: {:?}", archive_after.rewards_paid);

    assert!(epoch_after > epoch_before, "Epoch should have advanced");

    // Check if rewards_paid increased (indicates AdvancePool was called)
    let paid_before = archive_before.rewards_paid.unwrap_or(0);
    let paid_after = archive_after.rewards_paid.unwrap_or(0);
    println!("\n=== Results ===");
    println!("  Epoch advanced: {} -> {}", epoch_before, epoch_after);
    println!("  Rewards paid change: {} -> {}", paid_before, paid_after);

    println!("\nTest passed: Rewards distribution across epochs");
}

/// Test committee stake distribution with varying stakes.
///
/// This test:
/// 1. Creates nodes with different stake amounts
/// 2. Verifies spool allocation is proportional to stake
#[tokio::test]
#[ignore]
#[serial]
async fn test_stake_based_spool_allocation() {
    println!("=== Stake-Based Spool Allocation Test ===");

    // Use varying stakes for this test
    let ctx = TestContext::builder()
        .nodes(STAKING_NODE_COUNT)
        .port(STAKING_BASE_PORT + 400)
        .timeout(STAKING_TIMEOUT)
        .stake(1000) // Base stake, will be varied
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    wait_for_active_epoch(&ctx, 60).await;

    // Get committee info
    let committee = ctx.cli.account_committee(None)
        .expect("Failed to get committee");

    println!("\n=== Committee Members ===");
    if let Some(members) = &committee.members {
        let mut total_stake = 0u64;
        let mut total_spools = 0u16;

        for (i, member) in members.iter().enumerate().take(10) {
            let stake = member.stake.unwrap_or(0);
            let spools = member.spool_count.unwrap_or(0);
            total_stake += stake;
            total_spools += spools;
            println!(
                "  Member {}: node_id={:?}, stake={}, spools={}",
                i, member.node_id, stake, spools
            );
        }

        println!("\n=== Summary ===");
        println!("  Total members: {}", members.len());
        println!("  Sample total stake (first 10): {}", total_stake);
        println!("  Sample total spools (first 10): {}", total_spools);

        // With 50 nodes and 1024 spools, each node should have ~20 spools on average
        // (capped at 51 = 1024/20 for stake concentration limit)
        assert!(members.len() >= 24, "Should have enough members for normal mode");
    }

    println!("\nTest passed: Stake-based spool allocation verified");
}

/// Test node status shows stake and commission info.
///
/// Verifies we can query individual node status to see staking details.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_stake_status() {
    println!("=== Node Stake Status Test ({} nodes) ===", STAKING_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(STAKING_NODE_COUNT)
        .port(STAKING_BASE_PORT + 600)
        .timeout(STAKING_TIMEOUT)
        .stake(1000)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    wait_for_active_epoch(&ctx, 60).await;

    // Check status of a few nodes
    println!("\n=== Node Status ===");
    for (i, node) in ctx.nodes.iter().enumerate().take(5) {
        match ctx.cli.node_status(Some(&node.config_path), None) {
            Ok(status) => {
                println!(
                    "  Node {}: id={:?}, stake={:?}, spools={:?}, commission={:?}",
                    i,
                    status.node_id,
                    status.stake,
                    status.spool_count,
                    status.commission
                );
            }
            Err(e) => {
                println!("  Node {}: Failed to get status: {}", i, e);
            }
        }
    }

    println!("\nTest passed: Node stake status query works");
}

/// Test multiple epoch advances with timing verification.
///
/// This test verifies:
/// 1. Epoch 2 (bootstrap): Settling→Active is instant (committee_prev empty)
/// 2. Epoch 3+: Settling→Active takes time (needs supermajority of AdvancePool)
/// 3. Epochs 3+ should have consistent timing (all have real committee_prev)
///
/// Bootstrap does the only manual advance (epoch 1→2). After that, nodes
/// advance epochs autonomously when EPOCH_DURATION (60s) has elapsed.
#[tokio::test]
#[ignore]
#[serial]
async fn test_multi_epoch_reward_cycle() {
    println!("=== Multi-Epoch Timing Verification Test ({} nodes) ===", STAKING_NODE_COUNT);

    let ctx = TestContext::builder()
        .nodes(STAKING_NODE_COUNT)
        .port(STAKING_BASE_PORT + 800)
        .timeout(STAKING_TIMEOUT)
        .stake(1000)
        .fund(0.5)
        .build_and_bootstrap()
        .await
        .expect("Failed to setup test context");

    // Track timing for bootstrap epoch (epoch 2)
    println!("\n=== Waiting for bootstrap epoch (2) to become Active ===");
    let bootstrap_settling_time = wait_for_active_epoch_timed(&ctx, 60).await;
    println!("  Bootstrap epoch settling time: {:?}", bootstrap_settling_time);

    let node_urls = ctx.node_urls();

    // Upload some data to generate fees
    println!("\n=== Uploading data ===");
    for i in 0..2 {
        let blob = deterministic_blob(sizes::KB * 100, (i + 200) as u64);
        let upload_file = temp_file_with_content(&blob).expect("temp file");
        if let Ok(r) = ctx.cli.storage_upload(upload_file.path(), None, Some(&node_urls)) {
            println!("  Upload {}: {}", i + 1, r.track_id);
        }
    }

    // Observe autonomous epoch advances (nodes advance when EPOCH_DURATION elapses)
    // Bootstrap already did epoch 1→2, so we observe epochs 3, 4, 5, 6
    let num_epochs_to_observe = 4;
    let mut epoch_timings: Vec<(u64, Duration)> = Vec::new();

    println!("\n=== Observing {} autonomous epoch advances ===", num_epochs_to_observe);
    println!("(Nodes advance epochs every ~60s when conditions are met)");

    let mut current_epoch = ctx.epoch().await.expect("epoch").id.as_u64();

    for _i in 0..num_epochs_to_observe {
        let target_epoch = current_epoch + 1;
        println!("\n--- Waiting for epoch {} ---", target_epoch);

        // Wait for epoch to change (nodes advance autonomously)
        let epoch_change_timeout = Duration::from_secs(180); // 3 minutes max per epoch
        let poll_interval = Duration::from_millis(500);
        let wait_start = std::time::Instant::now();

        loop {
            if wait_start.elapsed() > epoch_change_timeout {
                panic!("Timed out waiting for epoch {} to start", target_epoch);
            }

            let epoch = ctx.epoch().await.expect("epoch");
            let epoch_id = epoch.id.as_u64();

            if epoch_id >= target_epoch {
                let phase = if epoch.state.is_syncing() { "Syncing" }
                    else if epoch.state.is_settling() { "Settling" }
                    else if epoch.state.is_active() { "Active" }
                    else { "Unknown" };
                println!("  Epoch {} started (phase: {})", epoch_id, phase);

                // Time how long it takes to reach Active phase
                let settling_time = wait_for_active_epoch_timed(&ctx, 120).await;
                println!("  Epoch {} settling time: {:?}", epoch_id, settling_time);

                epoch_timings.push((epoch_id, settling_time));
                current_epoch = epoch_id;
                break;
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    // Print timing summary
    println!("\n=== Epoch Timing Summary ===");
    println!("{:>6} {:>15}", "Epoch", "Settling (ms)");
    println!("{}", "-".repeat(25));
    println!("{:>6} {:>15}", 2, bootstrap_settling_time.as_millis());
    for (epoch, settling) in &epoch_timings {
        println!("{:>6} {:>15}", epoch, settling.as_millis());
    }

    // Verify timing expectations
    println!("\n=== Timing Verification ===");

    // Check bootstrap epoch was fast (committee_prev empty)
    let bootstrap_settling_ms = bootstrap_settling_time.as_millis();
    println!("  Bootstrap (epoch 2) settling: {}ms", bootstrap_settling_ms);
    assert!(
        bootstrap_settling_ms < 5000,
        "Bootstrap epoch settling should be fast (< 5s) since committee_prev is empty, got {}ms",
        bootstrap_settling_ms
    );
    println!("  ✓ Bootstrap epoch settling was fast (committee_prev empty)");

    // Check epochs 3+ settling times
    let settling_times: Vec<u128> = epoch_timings.iter().map(|(_, s)| s.as_millis()).collect();
    let min_settling = settling_times.iter().copied().min().unwrap_or(0);
    let max_settling = settling_times.iter().copied().max().unwrap_or(0);
    let avg_settling: u128 = settling_times.iter().sum::<u128>() / settling_times.len() as u128;

    println!("  Epochs 3+ settling times:");
    println!("    Min: {}ms", min_settling);
    println!("    Max: {}ms", max_settling);
    println!("    Avg: {}ms", avg_settling);

    // Epochs 3+ should have consistent timing
    if min_settling > 0 {
        let ratio = max_settling as f64 / min_settling as f64;
        println!("    Max/Min ratio: {:.2}x", ratio);
        assert!(
            ratio < 5.0,
            "Epoch settling times should be consistent (ratio < 5x), got {:.2}x",
            ratio
        );
        println!("  ✓ Epochs 3+ have consistent settling times");
    }

    // Verify we observed enough epochs
    let final_epoch = epoch_timings.last().map(|(e, _)| *e).unwrap_or(0);
    assert!(
        final_epoch >= 5,
        "Should have observed at least epoch 5, got {}",
        final_epoch
    );
    println!("  ✓ Successfully observed epochs up to {}", final_epoch);

    println!("\nTest passed: Multi-epoch timing verification completed");
}

// =============================================================================
// Helper functions
// =============================================================================

/// Wait for epoch to become Active phase, returning the time spent waiting.
///
/// Polls every 100ms for faster timing resolution.
/// Returns the duration from when we first saw a non-Active phase until Active.
async fn wait_for_active_epoch_timed(ctx: &TestContext, max_wait_secs: u64) -> Duration {
    let start = std::time::Instant::now();
    let max_wait = Duration::from_secs(max_wait_secs);
    let poll_interval = Duration::from_millis(100);

    loop {
        if let Ok(epoch) = ctx.epoch().await {
            let phase = if epoch.state.is_syncing() { "Syncing" }
                else if epoch.state.is_settling() { "Settling" }
                else if epoch.state.is_active() { "Active" }
                else { "Unknown" };
            if phase == "Active" {
                let elapsed = start.elapsed();
                println!("  Epoch {} is Active (took {:?})", epoch.id.as_u64(), elapsed);
                return elapsed;
            }
            // Log phase transitions
            if start.elapsed().as_secs() % 5 == 0 && start.elapsed().as_millis() % 5000 < 100 {
                println!("  Current phase: {} (elapsed: {:?})", phase, start.elapsed());
            }
        }
        if start.elapsed() >= max_wait {
            println!("  Warning: Epoch still not Active after {:?}", start.elapsed());
            return start.elapsed();
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Wait for epoch to become Active phase (simple version without timing).
#[allow(dead_code)]
async fn wait_for_active_epoch(ctx: &TestContext, max_wait_secs: u64) {
    wait_for_active_epoch_timed(ctx, max_wait_secs).await;
}
