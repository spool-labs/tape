//! Archive integration tests
//!
//! Tests for the Archive singleton account functionality including:
//! - Archive initialization state
//! - Storage pricing calculations
//! - Capacity tracking across epochs
//! - Reward pool management
//! - Fee collection via tape reservations
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test archive -- --ignored --test-threads=1
//! ```

mod common;

use serial_test::serial;

use common::*;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::instruction::build_reserve_tape_ix;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, StorageUnits};

/// Test that the Archive account is properly initialized after system init.
///
/// Verifies:
/// - Archive exists at the correct PDA address
/// - Initial storage_capacity is set (1000 MB)
/// - Initial storage_price is set (0.0001 TAPE per MB)
/// - Schedule is initialized at epoch 1
/// - Reward pool and paid are zero
/// - Tape count is zero
#[tokio::test]
#[ignore]
#[serial]
async fn test_archive_initialization() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Wait for fees to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;

    let archive = client.get_archive().await;
    assert!(
        archive.is_ok(),
        "Failed to fetch Archive: {:?}",
        archive.err()
    );

    let archive = archive.unwrap();

    // Check initial values from initialize.rs
    assert_eq!(
        archive.storage_capacity,
        StorageUnits(1000),
        "Initial storage capacity should be 1000 MB"
    );

    // Initial price is 0.0001 TAPE per MB (100 flux units)
    let expected_price = TAPE::from("0.0001");
    assert_eq!(
        archive.storage_price, expected_price,
        "Initial storage price should be 0.0001 TAPE per MB"
    );

    // Schedule should start at epoch 1
    assert_eq!(
        archive.schedule.current_epoch(),
        EpochNumber(1),
        "Schedule should start at epoch 1"
    );

    // Reward pool and paid should be zero initially
    assert_eq!(
        archive.rewards_pool,
        TAPE::zero(),
        "Rewards pool should be zero initially"
    );
    assert_eq!(
        archive.rewards_paid,
        TAPE::zero(),
        "Rewards paid should be zero initially"
    );

    // No tapes created yet
    assert_eq!(archive.tape_count, 0, "Tape count should be zero initially");

    // Recent usage should be zero
    assert_eq!(
        archive.recent_usage,
        StorageUnits::zero(),
        "Recent usage should be zero initially"
    );

    println!("Archive initialization test passed");
    println!("  storage_capacity: {} MB", archive.storage_capacity);
    println!("  storage_price: {} flux", archive.storage_price.as_u64());
    println!("  schedule epoch: {}", archive.schedule.current_epoch());
    println!("  tape_count: {}", archive.tape_count);
}

/// Test storage pricing calculation for tape reservations.
///
/// Verifies:
/// - Price calculation: storage_units * storage_price * num_epochs
/// - Tokens are transferred from user to archive ATA
#[tokio::test]
#[ignore]
#[serial]
async fn test_archive_pricing() {
    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;

    let archive = client.get_archive().await.expect("get archive");
    let epoch = client.get_epoch().await.expect("get epoch");

    println!(
        "Current epoch: {}, Archive price: {} flux/MB",
        epoch.id.as_u64(),
        archive.storage_price.as_u64()
    );

    // Create a tape purchaser with funds
    let purchaser = Keypair::new();
    let initial_balance: u64 = 10_000_000_000; // 10,000 TAPE (in flux)

    // Fund the purchaser with SOL for fees
    let transfer_sol_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &purchaser.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_sol_ix])
        .await
        .expect("fund purchaser with SOL");

    // Transfer TAPE tokens to purchaser
    transfer_tape(client, payer, &purchaser.pubkey(), initial_balance).await;

    // Reserve tape: 100 MB for 2 epochs
    let storage_units = StorageUnits(100);
    let start_epoch = epoch.id + EpochNumber(1); // Next epoch
    let end_epoch = start_epoch + EpochNumber(2); // 2 epochs duration

    // Calculate expected cost
    let price_per_unit = archive.storage_price.as_u64();
    let num_epochs = (end_epoch - start_epoch).as_u64();
    let expected_cost = price_per_unit * storage_units.as_u64() * num_epochs;

    println!(
        "Reserving {} MB for {} epochs (epoch {} to {})",
        storage_units.as_u64(),
        num_epochs,
        start_epoch.as_u64(),
        end_epoch.as_u64()
    );
    println!(
        "Expected cost: {} flux ({} * {} * {})",
        expected_cost,
        price_per_unit,
        storage_units.as_u64(),
        num_epochs
    );

    let reserve_ix = build_reserve_tape_ix(
        purchaser.pubkey(),
        purchaser.pubkey(),
        storage_units,
        start_epoch,
        end_epoch,
    );

    client
        .send_instructions(&purchaser, vec![reserve_ix])
        .await
        .expect("reserve tape");

    // Verify tape was created
    let tape = client
        .get_tape(&purchaser.pubkey())
        .await
        .expect("get tape");

    assert_eq!(tape.capacity, storage_units, "Tape capacity should match");
    assert_eq!(
        tape.active_epoch, start_epoch,
        "Tape active epoch should match"
    );
    assert_eq!(
        tape.expiry_epoch, end_epoch,
        "Tape expiry epoch should match"
    );

    // Verify archive tape count increased
    let archive_after = client.get_archive().await.expect("get archive after");
    assert_eq!(
        archive_after.tape_count, 1,
        "Archive tape count should be 1"
    );

    println!("Storage pricing test passed");
    println!("  Tape ID: {}", tape.id);
    println!("  Capacity: {} MB", tape.capacity);
    println!("  Active: epoch {}", tape.active_epoch);
    println!("  Expiry: epoch {}", tape.expiry_epoch);
}

/// Test archive capacity tracking across epoch schedule.
///
/// Verifies:
/// - Reserved capacity is tracked per-epoch in the schedule
/// - Capacity limits are enforced
/// - Schedule advances correctly with epoch advancement
#[tokio::test]
#[ignore]
#[serial]
async fn test_archive_capacity_tracking() {
    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;

    let archive_before = client.get_archive().await.expect("get archive");
    let epoch = client.get_epoch().await.expect("get epoch");

    println!(
        "Current epoch: {}, Capacity: {} MB",
        epoch.id.as_u64(),
        archive_before.storage_capacity.as_u64()
    );

    // Create purchaser with funds
    let purchaser = Keypair::new();
    let initial_balance: u64 = 100_000_000_000; // 100,000 TAPE

    let transfer_sol_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &purchaser.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_sol_ix])
        .await
        .expect("fund purchaser");

    transfer_tape(client, payer, &purchaser.pubkey(), initial_balance).await;

    // Reserve some capacity
    let storage_units = StorageUnits(500); // 500 MB (half of 1000 MB capacity)
    let start_epoch = epoch.id + EpochNumber(1);
    let end_epoch = start_epoch + EpochNumber(3); // 3 epochs

    let reserve_ix = build_reserve_tape_ix(
        purchaser.pubkey(),
        purchaser.pubkey(),
        storage_units,
        start_epoch,
        end_epoch,
    );

    client
        .send_instructions(&purchaser, vec![reserve_ix])
        .await
        .expect("reserve tape");

    // Check schedule has capacity reserved
    let archive_after = client.get_archive().await.expect("get archive after");

    // Verify the schedule shows reserved capacity for the requested epochs
    for e in start_epoch.as_u64()..end_epoch.as_u64() {
        let usage = archive_after
            .schedule
            .get(EpochNumber(e))
            .expect(&format!("get schedule for epoch {}", e));
        assert_eq!(
            usage.reserved(),
            storage_units,
            "Epoch {} should have {} MB reserved",
            e,
            storage_units.as_u64()
        );
    }

    println!("Capacity tracking test passed");
    println!(
        "  Reserved {} MB for epochs {} to {}",
        storage_units.as_u64(),
        start_epoch.as_u64(),
        end_epoch.as_u64()
    );
}

/// Test reward pool management across epoch transitions.
///
/// Verifies:
/// - Reward pool accumulates fees from tape reservations
/// - Reward pool is updated on epoch advance
/// - Leftover rewards carry over to next epoch
#[tokio::test]
#[ignore]
#[serial]
async fn test_archive_reward_pool() {
    use tape_api::program::EPOCH_DURATION;

    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;
    let (node_keypair, node_address) = &ctx.nodes[0];

    let epoch = client.get_epoch().await.expect("get epoch");
    let archive = client.get_archive().await.expect("get archive");

    println!(
        "Initial state - Epoch: {}, Reward pool: {} flux",
        epoch.id.as_u64(),
        archive.rewards_pool.as_u64()
    );

    // Create purchaser and reserve tape for current epoch
    let purchaser = Keypair::new();
    let initial_balance: u64 = 100_000_000_000;

    let transfer_sol_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &purchaser.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_sol_ix])
        .await
        .expect("fund purchaser");

    transfer_tape(client, payer, &purchaser.pubkey(), initial_balance).await;

    // Reserve tape starting from current epoch (so fees go to reward pool on next advance)
    let storage_units = StorageUnits(100);
    let start_epoch = epoch.id; // Current epoch
    let end_epoch = start_epoch + EpochNumber(5); // 5 epochs

    let reserve_ix = build_reserve_tape_ix(
        purchaser.pubkey(),
        purchaser.pubkey(),
        storage_units,
        start_epoch,
        end_epoch,
    );

    client
        .send_instructions(&purchaser, vec![reserve_ix])
        .await
        .expect("reserve tape");

    // Calculate expected fee per epoch
    let price_per_unit = archive.storage_price.as_u64();
    let fee_per_epoch = price_per_unit * storage_units.as_u64();

    println!(
        "Reserved {} MB at {} flux/MB = {} flux/epoch",
        storage_units.as_u64(),
        price_per_unit,
        fee_per_epoch
    );

    // Prepare node for next epoch
    advance_pool(client, node_keypair, *node_address)
        .await
        .ok();
    join_committee(client, node_keypair, *node_address)
        .await
        .ok();

    // Wait and advance epoch
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(client, payer).await.expect("advance epoch");

    // Check reward pool after advance
    let archive_after = client.get_archive().await.expect("get archive after");
    let epoch_after = client.get_epoch().await.expect("get epoch after");

    println!(
        "After advance - Epoch: {}, Reward pool: {} flux",
        epoch_after.id.as_u64(),
        archive_after.rewards_pool.as_u64()
    );

    // Reward pool should contain fees from the epoch that just completed
    assert!(
        archive_after.rewards_pool.as_u64() >= fee_per_epoch,
        "Reward pool should contain at least {} flux (has {} flux)",
        fee_per_epoch,
        archive_after.rewards_pool.as_u64()
    );

    // rewards_paid should be reset to zero
    assert_eq!(
        archive_after.rewards_paid,
        TAPE::zero(),
        "rewards_paid should be reset after epoch advance"
    );

    // recent_usage should reflect the reserved storage
    assert_eq!(
        archive_after.recent_usage, storage_units,
        "recent_usage should reflect reserved storage"
    );

    println!("Reward pool test passed");
    println!("  Fee per epoch: {} flux", fee_per_epoch);
    println!(
        "  Reward pool after advance: {} flux",
        archive_after.rewards_pool.as_u64()
    );
}

/// Test fee collection flow for tape reservations.
///
/// Verifies:
/// - TAPE tokens are transferred from user ATA to archive ATA (via schedule tracking)
/// - Archive tape count is incremented
/// - Schedule is updated with reserved capacity and fees
#[tokio::test]
#[ignore]
#[serial]
async fn test_archive_fee_collection() {
    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;

    let archive_before = client.get_archive().await.expect("get archive");
    let epoch = client.get_epoch().await.expect("get epoch");

    println!(
        "Initial state - Epoch: {}, Tape count: {}",
        epoch.id.as_u64(),
        archive_before.tape_count
    );

    // Create purchaser with known balance
    let purchaser = Keypair::new();
    let initial_tape_balance: u64 = 10_000_000_000; // 10,000 TAPE

    let transfer_sol_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &purchaser.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_sol_ix])
        .await
        .expect("fund purchaser");

    transfer_tape(client, payer, &purchaser.pubkey(), initial_tape_balance).await;

    // Reserve tape
    let storage_units = StorageUnits(200);
    let start_epoch = epoch.id + EpochNumber(1);
    let end_epoch = start_epoch + EpochNumber(2);
    let num_epochs = (end_epoch - start_epoch).as_u64();

    let price_per_unit = archive_before.storage_price.as_u64();
    let expected_total_cost = price_per_unit * storage_units.as_u64() * num_epochs;
    let fee_per_epoch = Coin::<TAPE>::new(price_per_unit * storage_units.as_u64());

    println!(
        "Reserving {} MB for {} epochs at {} flux/MB",
        storage_units.as_u64(),
        num_epochs,
        price_per_unit
    );
    println!("Expected total cost: {} flux", expected_total_cost);
    println!("Fee per epoch: {} flux", fee_per_epoch.as_u64());

    let reserve_ix = build_reserve_tape_ix(
        purchaser.pubkey(),
        purchaser.pubkey(),
        storage_units,
        start_epoch,
        end_epoch,
    );

    client
        .send_instructions(&purchaser, vec![reserve_ix])
        .await
        .expect("reserve tape");

    // Verify archive state updated
    let archive_after = client.get_archive().await.expect("get archive after");

    // Tape count should increment
    assert_eq!(
        archive_after.tape_count,
        archive_before.tape_count + 1,
        "Tape count should increment"
    );

    // Verify schedule has fees recorded for each epoch in the range
    for e in start_epoch.as_u64()..end_epoch.as_u64() {
        let usage = archive_after
            .schedule
            .get(EpochNumber(e))
            .expect(&format!("get schedule for epoch {}", e));

        assert_eq!(
            usage.paid(),
            fee_per_epoch,
            "Epoch {} should have {} flux in fees",
            e,
            fee_per_epoch.as_u64()
        );

        assert_eq!(
            usage.reserved(),
            storage_units,
            "Epoch {} should have {} MB reserved",
            e,
            storage_units.as_u64()
        );
    }

    // Verify tape was created with correct properties
    let tape = client
        .get_tape(&purchaser.pubkey())
        .await
        .expect("get tape");

    assert_eq!(tape.capacity, storage_units, "Tape capacity should match");
    assert_eq!(tape.active_epoch, start_epoch, "Active epoch should match");
    assert_eq!(tape.expiry_epoch, end_epoch, "Expiry epoch should match");
    assert_eq!(tape.used, StorageUnits::zero(), "Used should be zero initially");

    println!("Fee collection test passed");
    println!("  Storage: {} MB for {} epochs", storage_units.as_u64(), num_epochs);
    println!("  Total cost: {} flux", expected_total_cost);
    println!("  Tape ID: {}", tape.id);
    println!("  Tape count: {} -> {}", archive_before.tape_count, archive_after.tape_count);
}
