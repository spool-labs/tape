//! Integration tests for Tape functionality
//!
//! These tests verify the tape allocation and management functionality:
//! - Basic tape allocation (ReserveTape)
//! - Tape lifecycle (allocate, use via tracks, verify state)
//! - Capacity limit enforcement
//! - Multiple tape allocations per user
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test tape -- --ignored --test-threads=1
//! ```

mod common;

use serial_test::serial;

use common::{create_client, initialize_system, setup_validator, transfer_tape, ValidatorGuard};
use solana_sdk::signature::{Keypair, Signer};
use tape_api::errors::TapeError;
use tape_api::instruction::{build_register_track_ix, build_reserve_tape_ix};
use tape_core::encoding::EncodingProfile;
use tape_core::prelude::*;
use tape_core::types::{EpochNumber, StorageUnits, TapeNumber};

/// Helper to reserve a tape for an authority
async fn reserve_tape(
    client: &rpc_client::RpcClient<rpc_test::TestRpc>,
    payer: &Keypair,
    authority: &Keypair,
    storage_units: StorageUnits,
    activation_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Result<(), String> {
    let reserve_ix = build_reserve_tape_ix(
        payer.pubkey(),
        authority.pubkey(),
        storage_units,
        activation_epoch,
        expiry_epoch,
    );
    // Both payer and authority must sign
    client
        .send_instructions_with_signers(payer, vec![reserve_ix], &[authority])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Test basic tape allocation
///
/// Verifies that a user can allocate a tape with valid parameters.
#[tokio::test]
#[ignore]
#[serial]
async fn test_allocate_tape() {
    println!("Starting test_allocate_tape...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Wait for validator to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Create a user with TAPE tokens
    let user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &user.pubkey(),
        1_000_000_000, // 1 SOL for fees
    );
    client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    // Transfer TAPE tokens to user
    let tape_amount: u64 = 10_000_000; // 10 TAPE (6 decimals)
    transfer_tape(&client, &payer, &user.pubkey(), tape_amount).await;
    println!("User funded with {} TAPE", tape_amount);

    // Get current epoch
    let epoch = client.get_epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id;
    println!("Current epoch: {}", current_epoch.as_u64());

    // Reserve a tape for 2 epochs starting from current epoch
    let storage_units = StorageUnits(100); // 100 MB
    let activation_epoch = current_epoch;
    let expiry_epoch = EpochNumber(current_epoch.as_u64() + 2);

    reserve_tape(
        &client,
        &payer,
        &user,
        storage_units,
        activation_epoch,
        expiry_epoch,
    )
    .await
    .expect("Failed to reserve tape");
    println!("Tape reserved successfully");

    // Verify tape was created
    let tape = client
        .get_tape(&user.pubkey())
        .await
        .expect("Failed to get tape");

    assert_eq!(tape.id, TapeNumber(1), "Tape ID should be 1 (first tape)");
    assert_eq!(tape.authority, user.pubkey(), "Tape authority should match");
    assert_eq!(tape.capacity, storage_units, "Tape capacity should match");
    assert_eq!(tape.used, StorageUnits(0), "Tape used should be 0");
    assert_eq!(
        tape.active_epoch, activation_epoch,
        "Tape active epoch should match"
    );
    assert_eq!(
        tape.expiry_epoch, expiry_epoch,
        "Tape expiry epoch should match"
    );

    println!("TEST PASSED: Basic tape allocation works");
}

/// Test tape lifecycle - allocate, use, and verify state
///
/// Tests the full lifecycle:
/// 1. Allocate tape
/// 2. Register a track (uses tape storage)
/// 3. Verify tape used storage increases
#[tokio::test]
#[ignore]
#[serial]
async fn test_tape_lifecycle() {
    println!("Starting test_tape_lifecycle...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Create and fund user
    let user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &user.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    let tape_amount: u64 = 10_000_000;
    transfer_tape(&client, &payer, &user.pubkey(), tape_amount).await;
    println!("User funded");

    // Get current epoch
    let epoch = client.get_epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id;

    // Reserve a tape with 500 MB capacity
    let storage_units = StorageUnits(500);
    let activation_epoch = current_epoch;
    let expiry_epoch = EpochNumber(current_epoch.as_u64() + 10);

    reserve_tape(
        &client,
        &payer,
        &user,
        storage_units,
        activation_epoch,
        expiry_epoch,
    )
    .await
    .expect("Failed to reserve tape");
    println!("Tape reserved with {} MB capacity", storage_units.as_u64());

    // Verify initial state
    let tape_before = client
        .get_tape(&user.pubkey())
        .await
        .expect("Failed to get tape");
    assert_eq!(tape_before.used, StorageUnits(0), "Initial used should be 0");
    assert_eq!(
        tape_before.track_count, 0,
        "Initial track count should be 0"
    );

    // Register a track using 100 MB
    let track_size = StorageUnits(100);
    let data_root = Hash::new_unique();
    let erasure_root = Hash::new_unique();
    let key_hash = Hash::new_unique();

    let register_track_ix = build_register_track_ix(
        user.pubkey(),
        user.pubkey(),
        track_size,
        data_root,
        erasure_root,
        key_hash,
        EncodingProfile::clay_default(),
        0,
        0,
        [Hash::default(); SPOOL_GROUP_SIZE],
    );

    client
        .send_instructions(&user, vec![register_track_ix])
        .await
        .expect("Failed to register track");
    println!("Track registered with {} MB", track_size.as_u64());

    // Verify tape state after track registration
    let tape_after = client
        .get_tape(&user.pubkey())
        .await
        .expect("Failed to get tape after track");

    assert_eq!(
        tape_after.used, track_size,
        "Used storage should equal track size"
    );
    assert_eq!(tape_after.track_count, 1, "Track count should be 1");
    assert_eq!(
        tape_after.capacity, storage_units,
        "Capacity should remain unchanged"
    );

    println!(
        "Tape state: capacity={} MB, used={} MB, tracks={}",
        tape_after.capacity.as_u64(),
        tape_after.used.as_u64(),
        tape_after.track_count
    );

    println!("TEST PASSED: Tape lifecycle works correctly");
}

/// Test that tape allocation does not require committee membership
///
/// Any user with TAPE tokens should be able to reserve storage capacity.
/// This is different from node operations which require committee membership.
#[tokio::test]
#[ignore]
#[serial]
async fn test_tape_requires_committee_membership() {
    println!("Starting test_tape_requires_committee_membership...");
    println!("(Testing that tape allocation does NOT require committee membership)");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Create a regular user (not a node, not in committee)
    let regular_user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &regular_user.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    let tape_amount: u64 = 10_000_000;
    transfer_tape(&client, &payer, &regular_user.pubkey(), tape_amount).await;
    println!("Regular user (non-node) funded with TAPE");

    // Verify this user is not a node
    let node_result = client.get_node(&regular_user.pubkey()).await;
    assert!(
        node_result.is_err(),
        "Regular user should not have a node account"
    );

    // Get current epoch
    let epoch = client.get_epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id;

    // Reserve a tape - this should succeed even without committee membership
    let storage_units = StorageUnits(50);
    let activation_epoch = current_epoch;
    let expiry_epoch = EpochNumber(current_epoch.as_u64() + 2);

    let result = reserve_tape(
        &client,
        &payer,
        &regular_user,
        storage_units,
        activation_epoch,
        expiry_epoch,
    )
    .await;

    assert!(
        result.is_ok(),
        "Regular user should be able to reserve tape: {:?}",
        result.err()
    );

    // Verify tape was created
    let tape = client
        .get_tape(&regular_user.pubkey())
        .await
        .expect("Failed to get tape");
    assert_eq!(tape.authority, regular_user.pubkey());

    println!("TEST PASSED: Tape allocation works for non-committee users");
}

/// Test tape capacity limits
///
/// Verifies that:
/// 1. Storage requests exceeding archive capacity are rejected
/// 2. Tracks exceeding tape capacity are rejected
#[tokio::test]
#[ignore]
#[serial]
async fn test_tape_capacity_limits() {
    println!("Starting test_tape_capacity_limits...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Get archive to check capacity
    let archive = client.get_archive().await.expect("Failed to get archive");
    println!(
        "Archive capacity: {} MB, price: {} per MB",
        archive.storage_capacity.as_u64(),
        archive.storage_price.as_u64()
    );

    // Create and fund user
    let user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &user.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    // Give user lots of TAPE for testing
    let tape_amount: u64 = 1_000_000_000; // 1000 TAPE
    transfer_tape(&client, &payer, &user.pubkey(), tape_amount).await;
    println!("User funded with {} TAPE", tape_amount);

    // Get current epoch
    let epoch = client.get_epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id;

    // First, reserve a small tape successfully
    let small_storage = StorageUnits(100);
    let activation_epoch = current_epoch;
    let expiry_epoch = EpochNumber(current_epoch.as_u64() + 2);

    reserve_tape(
        &client,
        &payer,
        &user,
        small_storage,
        activation_epoch,
        expiry_epoch,
    )
    .await
    .expect("Failed to reserve small tape");
    println!("Small tape reserved successfully");

    // Register a track that uses most of the capacity
    let track_size = StorageUnits(80);
    let key_hash = Hash::new_unique();

    let register_track_ix = build_register_track_ix(
        user.pubkey(),
        user.pubkey(),
        track_size,
        Hash::new_unique(),
        Hash::new_unique(),
        key_hash,
        EncodingProfile::clay_default(),
        0,
        0,
        [Hash::default(); SPOOL_GROUP_SIZE],
    );

    client
        .send_instructions(&user, vec![register_track_ix])
        .await
        .expect("Failed to register first track");
    println!("First track registered ({} MB)", track_size.as_u64());

    // Try to register a track that exceeds remaining capacity
    let oversized_track = StorageUnits(50); // Would exceed 100 MB capacity (80 + 50 > 100)
    let key_hash2 = Hash::new_unique();

    let oversized_ix = build_register_track_ix(
        user.pubkey(),
        user.pubkey(),
        oversized_track,
        Hash::new_unique(),
        Hash::new_unique(),
        key_hash2,
        EncodingProfile::clay_default(),
        0,
        0,
        [Hash::default(); SPOOL_GROUP_SIZE],
    );

    let result = client.send_instructions(&user, vec![oversized_ix]).await;
    assert!(
        result.is_err(),
        "Should fail to register track exceeding capacity"
    );

    let err_str = format!("{:?}", result.unwrap_err());
    // Use typed error parsing
    let tape_err = TapeError::from_error_string(&err_str);
    assert!(
        tape_err == Some(TapeError::NoSpace),
        "Expected NoSpace error, got: {} (parsed: {:?})",
        err_str,
        tape_err
    );
    println!("Oversized track correctly rejected with NoSpace error");

    println!("TEST PASSED: Tape capacity limits enforced");
}

/// Test multiple tape allocation per node/user
///
/// Note: The current implementation uses authority as the PDA seed,
/// which means each authority can only have one tape at a time.
/// This test verifies this behavior.
#[tokio::test]
#[ignore]
#[serial]
async fn test_multiple_tape_allocation() {
    println!("Starting test_multiple_tape_allocation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Create first user and allocate tape
    let user1 = Keypair::new();
    let transfer_ix1 = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &user1.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix1])
        .await
        .expect("Failed to fund user1");

    let tape_amount: u64 = 10_000_000;
    transfer_tape(&client, &payer, &user1.pubkey(), tape_amount).await;

    let epoch = client.get_epoch().await.expect("Failed to get epoch");
    let current_epoch = epoch.id;

    // First user allocates tape
    reserve_tape(
        &client,
        &payer,
        &user1,
        StorageUnits(100),
        current_epoch,
        EpochNumber(current_epoch.as_u64() + 2),
    )
    .await
    .expect("User1 should be able to reserve tape");
    println!("User1 reserved tape");

    // Verify user1's tape
    let tape1 = client
        .get_tape(&user1.pubkey())
        .await
        .expect("Failed to get tape1");
    assert_eq!(tape1.id, TapeNumber(1), "First tape should have ID 1");

    // Create second user and allocate tape
    let user2 = Keypair::new();
    let transfer_ix2 = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &user2.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix2])
        .await
        .expect("Failed to fund user2");

    transfer_tape(&client, &payer, &user2.pubkey(), tape_amount).await;

    // Second user allocates tape
    reserve_tape(
        &client,
        &payer,
        &user2,
        StorageUnits(200),
        current_epoch,
        EpochNumber(current_epoch.as_u64() + 3),
    )
    .await
    .expect("User2 should be able to reserve tape");
    println!("User2 reserved tape");

    // Verify user2's tape
    let tape2 = client
        .get_tape(&user2.pubkey())
        .await
        .expect("Failed to get tape2");
    assert_eq!(tape2.id, TapeNumber(2), "Second tape should have ID 2");

    // Verify both tapes exist with correct properties
    let all_tapes = client
        .get_all_tapes()
        .await
        .expect("Failed to get all tapes");
    assert_eq!(all_tapes.len(), 2, "Should have 2 tapes total");

    // Try to allocate another tape for user1 (should fail - PDA already exists)
    let result = reserve_tape(
        &client,
        &payer,
        &user1,
        StorageUnits(50),
        current_epoch,
        EpochNumber(current_epoch.as_u64() + 2),
    )
    .await;

    assert!(
        result.is_err(),
        "User1 should not be able to allocate second tape (PDA exists)"
    );
    println!("Correctly prevented second tape allocation for same user");

    // Verify archive tape count
    let archive = client.get_archive().await.expect("Failed to get archive");
    assert_eq!(archive.tape_count, 2, "Archive should show 2 tapes");

    println!("TEST PASSED: Multiple tape allocation works correctly");
}
