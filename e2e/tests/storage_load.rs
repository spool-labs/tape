//! Storage load test - continuously add tapes to generate fees.
//!
//! Sets up 42 nodes and adds 1MB files until 100MB total storage is used.
//! Creates a single tape lasting 100 epochs to generate sustained fees.
//! Randomly adds/removes stake from nodes each epoch.
//!
//! ```bash
//! cargo test -p tape-e2e --test storage_load -- --ignored --nocapture
//! ```

use std::time::Duration;

use rand::Rng;
use serial_test::serial;
use solana_sdk::pubkey::Pubkey;
use tape_core::types::EpochNumber;
use tape_e2e::{
    TestContext, deterministic_blob, temp_file_with_content, sizes,
};

/// Number of nodes for this test.
const NODE_COUNT: usize = 42;

/// Base port.
const BASE_PORT: u16 = 12000;

/// Timeout for setup.
const TIMEOUT: Duration = Duration::from_secs(600);

/// Target total storage in MB.
const TARGET_MB: u64 = 100;

/// Size of each upload in bytes.
const UPLOAD_SIZE: usize = sizes::MB;

/// Number of epochs the tape should last.
const TAPE_EPOCHS: u64 = 100;

/// Stake amount for random operations (in TAPE tokens).
const STAKE_AMOUNT: u64 = 100;

/// Load test: add 1MB uploads to a 100-epoch tape until 100MB total.
#[tokio::test]
#[ignore]
#[serial]
async fn test_storage_load() {
    println!("=== Storage Load Test ===");
    println!("Nodes: {}", NODE_COUNT);
    println!("Target: {} MB over {} epochs", TARGET_MB, TAPE_EPOCHS);
    println!();

    // Bootstrap to epoch 4 (past bootstrap period)
    println!("Setting up {} nodes...", NODE_COUNT);
    let ctx = TestContext::builder()
        .nodes(NODE_COUNT)
        .port(BASE_PORT)
        .timeout(TIMEOUT)
        .fund(1.0)
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap");

    let epoch = ctx.epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id.as_u64();
    println!("Bootstrap complete at epoch {}", current_epoch);

    let archive = ctx.archive().await.expect("Failed to get archive");
    let price_per_mb_per_epoch = archive.storage_price.as_u64();
    println!("Storage price: {} flux/MB/epoch", price_per_mb_per_epoch);

    // Calculate expected fees
    let total_fees = price_per_mb_per_epoch * TARGET_MB * TAPE_EPOCHS;
    println!("Expected total fees: {} flux ({} TAPE)", total_fees, total_fees / 1_000_000);
    println!();

    // Create a single tape for 100 MB lasting 100 epochs
    let start_epoch = EpochNumber(current_epoch);
    let end_epoch = EpochNumber(current_epoch + TAPE_EPOCHS);

    println!("Creating tape: {} MB, epochs {}-{}", TARGET_MB, start_epoch.0, end_epoch.0);
    let tape = ctx.cli.tape_init(TARGET_MB, start_epoch, end_epoch)
        .expect("Failed to create tape");
    println!("  Tape authority: {}", tape);
    println!();

    // Get node URLs for uploads
    let node_urls = ctx.node_urls();
    let upload_nodes: Vec<String> = node_urls.iter().take(30).cloned().collect();

    // Collect node addresses for staking
    let node_addresses: Vec<Pubkey> = ctx.nodes.iter()
        .filter_map(|n| n.node_address)
        .collect();
    println!("Node addresses available for staking: {}", node_addresses.len());

    println!("Uploading {} MB in 1 MB increments...", TARGET_MB);
    println!();

    let mut total_uploaded = 0usize;
    let mut upload_count = 0u64;
    let mut consecutive_failures = 0u32;
    const MAX_CONSECUTIVE_FAILURES: u32 = 10;

    while total_uploaded < (TARGET_MB as usize) * sizes::MB {
        upload_count += 1;
        let seed = upload_count;
        let blob = deterministic_blob(UPLOAD_SIZE, seed);
        let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

        match ctx.cli.storage_upload(upload_file.path(), Some(&tape), Some(&upload_nodes)) {
            Ok(result) => {
                total_uploaded += blob.len();
                consecutive_failures = 0;
                let mb = total_uploaded / sizes::MB;
                println!(
                    "  Upload {}: {} ({} MB total)",
                    upload_count,
                    &result.track_id[..16],
                    mb
                );
            }
            Err(e) => {
                consecutive_failures += 1;
                println!("  Upload {} failed: {}", upload_count, e);
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    println!("  Too many consecutive failures, skipping upload phase");
                    break;
                }
            }
        }

        // Brief pause between uploads
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!();
    println!("Upload phase complete:");
    println!("  Uploads: {}", upload_count);
    println!("  Total: {} MB", total_uploaded / sizes::MB);

    // Fetch final state
    let archive = ctx.archive().await.expect("Failed to get archive");
    let epoch = ctx.epoch().await.expect("Failed to get epoch");

    println!();
    println!("Current state:");
    println!("  Epoch: {}", epoch.id.as_u64());
    println!("  Storage capacity: {} MB", archive.storage_capacity.as_u64());
    println!("  Recent usage: {} MB", archive.recent_usage.as_u64());
    println!("  Rewards pool: {} flux", archive.rewards_pool.as_u64());
    println!("  Rewards paid: {} flux", archive.rewards_paid.as_u64());

    // Keep running for observation (advance epochs to see reward distribution)
    println!();
    println!("Advancing epochs with random stake operations...");
    println!("(Press Ctrl+C to stop, or wait for 20 epochs)");
    println!();

    // Track stake accounts we've created for potential unlocks
    let mut stake_accounts: Vec<Pubkey> = Vec::new();
    let mut rng = rand::thread_rng();

    for _ in 1..=20 {
        // Wait for epoch to advance
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Random stake operation
        let do_stake = stake_accounts.is_empty() || rng.gen_bool(0.6); // 60% chance to stake

        if do_stake && !node_addresses.is_empty() {
            // Stake 100 TAPE to a random node
            let node_idx = rng.gen_range(0..node_addresses.len());
            let node = node_addresses[node_idx];

            match ctx.cli.stake_deposit(&node, STAKE_AMOUNT) {
                Ok(stake_account) => {
                    println!("    [STAKE] +{} TAPE to node {} (addr: {}) -> stake account: {}",
                        STAKE_AMOUNT, node_idx, &node.to_string()[..8], &stake_account.to_string()[..8]);
                    stake_accounts.push(stake_account);
                }
                Err(e) => {
                    println!("    [STAKE] Failed to stake {} TAPE to node {} ({}): {}",
                        STAKE_AMOUNT, node_idx, &node.to_string()[..8], e);
                }
            }
        } else if !stake_accounts.is_empty() {
            // Unlock a random stake account
            let stake_idx = rng.gen_range(0..stake_accounts.len());
            let stake = stake_accounts[stake_idx];

            match ctx.cli.stake_unlock(&stake) {
                Ok(()) => {
                    println!("    [UNLOCK] Requested unlock for {}", &stake.to_string()[..8]);
                    // Remove from list (can't unlock again)
                    stake_accounts.remove(stake_idx);
                }
                Err(e) => {
                    println!("    [UNLOCK] Failed to unlock {}: {}", &stake.to_string()[..8], e);
                }
            }
        }

        // Print epoch state
        if let Ok(new_epoch) = ctx.epoch().await {
            if let Ok(new_archive) = ctx.archive().await {
                println!(
                    "  Epoch {:>3}: pool={:>10} flux, paid={:>10} flux, usage={:>3} MB",
                    new_epoch.id.as_u64(),
                    new_archive.rewards_pool.as_u64(),
                    new_archive.rewards_paid.as_u64(),
                    new_archive.recent_usage.as_u64(),
                );
            }
        }
    }

    println!();
    println!("=== Test Complete ===");
    println!();
    println!("The tape will continue generating {} flux/epoch for {} more epochs.",
        price_per_mb_per_epoch * TARGET_MB,
        TAPE_EPOCHS - 20);
    println!("Monitor the network with: tape-monitor -u l");
}
