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
    println!("Storage load test: {} nodes, {} MB over {} epochs", NODE_COUNT, TARGET_MB, TAPE_EPOCHS);

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

    // Track stake accounts we've created for potential unlocks
    let mut stake_accounts: Vec<Pubkey> = Vec::new();
    let mut rng = rand::thread_rng();

    let mut total_uploaded = 0usize;
    let mut upload_count = 0u64;
    let mut successful_uploads = 0u64;

    println!("Starting upload + stake loop (target: {} MB, {} iterations)", TARGET_MB, TAPE_EPOCHS);
    println!();

    // Interleave uploads with stake operations - one upload attempt, then stake change, repeat
    for iteration in 1..=TAPE_EPOCHS {
        // 1. Try one upload
        upload_count += 1;
        let seed = upload_count;
        let blob = deterministic_blob(UPLOAD_SIZE, seed);
        let upload_file = temp_file_with_content(&blob).expect("Failed to create temp file");

        match ctx.cli.storage_upload(upload_file.path(), Some(&tape), Some(&upload_nodes)) {
            Ok(result) => {
                total_uploaded += blob.len();
                successful_uploads += 1;
                let mb = total_uploaded / sizes::MB;
                println!(
                    "  [UPLOAD] #{}: {} ({} MB total)",
                    upload_count,
                    &result.track_id[..16],
                    mb
                );
            }
            Err(e) => {
                println!("  [UPLOAD] #{} failed: {}", upload_count, e);
            }
        }

        // 2. Random stake operation
        let do_stake = stake_accounts.is_empty() || rng.gen_bool(0.6); // 60% chance to stake

        if do_stake && !node_addresses.is_empty() {
            // Stake 100 TAPE to a random node
            let node_idx = rng.gen_range(0..node_addresses.len());
            let node = node_addresses[node_idx];

            match ctx.cli.stake_deposit(&node, STAKE_AMOUNT) {
                Ok(stake_account) => {
                    println!("    [STAKE] +{} TAPE to node {} -> {}",
                        STAKE_AMOUNT, node_idx, &stake_account.to_string()[..8]);
                    stake_accounts.push(stake_account);
                }
                Err(e) => {
                    println!("    [STAKE] Failed: {}", e);
                }
            }
        } else if !stake_accounts.is_empty() {
            // Unlock a random stake account
            let stake_idx = rng.gen_range(0..stake_accounts.len());
            let stake = stake_accounts[stake_idx];

            match ctx.cli.stake_unlock(&stake) {
                Ok(()) => {
                    println!("    [UNLOCK] {}", &stake.to_string()[..8]);
                    stake_accounts.remove(stake_idx);
                }
                Err(e) => {
                    println!("    [UNLOCK] Failed {}: {}", &stake.to_string()[..8], e);
                }
            }
        }

        // 3. Print epoch state periodically
        if iteration % 5 == 0 {
            if let Ok(new_epoch) = ctx.epoch().await {
                if let Ok(new_archive) = ctx.archive().await {
                    println!(
                        "  --- Iter {:>3} | Epoch {:>3} | pool={:>10} flux | usage={:>3} MB | uploads={}/{} ---",
                        iteration,
                        new_epoch.id.as_u64(),
                        new_archive.rewards_pool.as_u64(),
                        new_archive.recent_usage.as_u64(),
                        successful_uploads,
                        upload_count,
                    );
                }
            }
        }

        // Brief pause between iterations
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Fetch final state
    let archive = ctx.archive().await.expect("Failed to get archive");
    let epoch = ctx.epoch().await.expect("Failed to get epoch");

    println!();
    println!("Test complete:");
    println!("  Iterations: {}", TAPE_EPOCHS);
    println!("  Uploads attempted: {}", upload_count);
    println!("  Uploads succeeded: {}", successful_uploads);
    println!("  Total uploaded: {} MB", total_uploaded / sizes::MB);
    println!("  Final epoch: {}", epoch.id.as_u64());
    println!("  Storage capacity: {} MB", archive.storage_capacity.as_u64());
    println!("  Recent usage: {} MB", archive.recent_usage.as_u64());
    println!("  Rewards pool: {} flux", archive.rewards_pool.as_u64());
    println!("  Rewards paid: {} flux", archive.rewards_paid.as_u64());
}
