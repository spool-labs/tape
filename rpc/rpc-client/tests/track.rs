//! Integration tests for Track functionality
//!
//! These tests verify track registration, metadata storage, and multiple tracks per tape.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test track -- --ignored --nocapture
//! ```

mod common;

use common::{
    create_client, initialize_system, setup_validator, ValidatorGuard,
};
use solana_sdk::signature::{Keypair, Signer};
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{build_register_track_ix, build_reserve_tape_ix};
use tape_api::program::tapedrive::tape_pda;
use tape_core::prelude::*;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, StorageUnits};

/// Helper to reserve a tape for testing.
/// Returns (tape_authority_keypair, tape_address)
async fn reserve_test_tape(
    client: &rpc_client::RpcClient<rpc_test::TestRpc>,
    payer: &Keypair,
    storage_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> (Keypair, solana_sdk::pubkey::Pubkey) {
    // Generate a unique keypair for this tape
    let tape_authority = Keypair::new();
    let (tape_address, _) = tape_pda(tape_authority.pubkey());

    // Calculate cost: storage_units * price * epochs (generous allocation for safety)
    let epochs = end_epoch.as_u64().saturating_sub(start_epoch.as_u64()).max(1);
    let cost = Coin::<TAPE>::new(storage_units.as_u64() * 200 * epochs); // 200 per MB per epoch buffer

    // Build instructions to create ATA and transfer tokens
    let mut instructions = build_authority_with_tokens_ix(
        payer.pubkey(),
        tape_authority.pubkey(),
        cost,
    );

    // Add reserve tape instruction
    instructions.push(build_reserve_tape_ix(
        payer.pubkey(),
        tape_authority.pubkey(),
        storage_units,
        start_epoch,
        end_epoch,
    ));

    // Send with tape authority as additional signer
    client
        .send_instructions_with_signers(payer, instructions, &[&tape_authority])
        .await
        .expect("Failed to reserve tape");

    (tape_authority, tape_address)
}

/// Test basic track write (registration) operation.
///
/// Verifies that:
/// 1. A track can be registered on an existing tape
/// 2. The track account is created with correct data
/// 3. The tape's track count is incremented
#[tokio::test]
#[ignore]
async fn test_write_track() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Wait for validator to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;

    // Reserve a tape (epochs 1-10, 100 MB)
    let (tape_authority, tape_address) = reserve_test_tape(
        &client,
        &payer,
        StorageUnits(100),
        EpochNumber(1),
        EpochNumber(10),
    )
    .await;

    // Verify tape was created
    let tape = client
        .get_tape(&tape_authority.pubkey())
        .await
        .expect("Failed to fetch tape");
    assert_eq!(tape.track_count, 0, "New tape should have no tracks");

    // Register a track
    let key_hash = Hash::from([1u8; 32]);
    let root_hash = Hash::from([2u8; 32]);
    let commitment_hash = Hash::from([3u8; 32]);
    let track_size = StorageUnits(10);

    let register_ix = build_register_track_ix(
        payer.pubkey(),
        tape_authority.pubkey(),
        track_size,
        root_hash,
        commitment_hash,
        key_hash,
    );

    client
        .send_instructions_with_signers(&payer, vec![register_ix], &[&tape_authority])
        .await
        .expect("Failed to register track");

    // Verify track was created
    let track = client
        .get_track(&tape_authority.pubkey(), &key_hash)
        .await
        .expect("Failed to fetch track");

    assert_eq!(track.tape, tape_address, "Track should reference tape");
    assert_eq!(track.key, key_hash, "Track key should match");
    assert_eq!(track.size, track_size, "Track size should match");

    // Verify tape track count was incremented
    let tape = client
        .get_tape(&tape_authority.pubkey())
        .await
        .expect("Failed to fetch tape");
    assert_eq!(tape.track_count, 1, "Tape should have 1 track");

    println!("Track registered successfully!");
    println!("  Tape: {}", tape_address);
    println!("  Track ID: {}", track.id.as_u64());
    println!("  Size: {} MB", track.size.as_u64());
}

/// Test that track registration requires an allocated tape.
///
/// Verifies that attempting to register a track without a tape fails.
#[tokio::test]
#[ignore]
async fn test_track_requires_tape() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;

    // Create a keypair for a non-existent tape
    let fake_tape_authority = Keypair::new();

    let key_hash = Hash::from([1u8; 32]);
    let root_hash = Hash::from([2u8; 32]);
    let commitment_hash = Hash::from([3u8; 32]);
    let track_size = StorageUnits(10);

    let register_ix = build_register_track_ix(
        payer.pubkey(),
        fake_tape_authority.pubkey(),
        track_size,
        root_hash,
        commitment_hash,
        key_hash,
    );

    // This should fail because no tape exists
    let result = client
        .send_instructions_with_signers(&payer, vec![register_ix], &[&fake_tape_authority])
        .await;

    assert!(result.is_err(), "Track registration should fail without tape");
    println!("Track registration correctly rejected without tape");
}

/// Test that track metadata (key hash, size) is stored correctly.
///
/// Verifies that all track fields match the registration parameters.
#[tokio::test]
#[ignore]
async fn test_track_metadata_storage() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;

    // Reserve a tape
    let (tape_authority, tape_address) = reserve_test_tape(
        &client,
        &payer,
        StorageUnits(500),
        EpochNumber(1),
        EpochNumber(20),
    )
    .await;

    // Use specific values to verify storage
    let key_hash = Hash::from([42u8; 32]);
    let root_hash = Hash::from([123u8; 32]);
    let commitment_hash = Hash::from([200u8; 32]);
    let track_size = StorageUnits(50);

    let register_ix = build_register_track_ix(
        payer.pubkey(),
        tape_authority.pubkey(),
        track_size,
        root_hash,
        commitment_hash,
        key_hash,
    );

    client
        .send_instructions_with_signers(&payer, vec![register_ix], &[&tape_authority])
        .await
        .expect("Failed to register track");

    // Fetch and verify track metadata
    let track = client
        .get_track(&tape_authority.pubkey(), &key_hash)
        .await
        .expect("Failed to fetch track");

    // Verify all fields
    assert_eq!(track.tape, tape_address, "Tape address mismatch");
    assert_eq!(track.key, key_hash, "Key hash mismatch");
    assert_eq!(track.size, track_size, "Size mismatch");

    // Verify track data contains commitment hash
    assert_eq!(
        track.data.commitment_hash, commitment_hash,
        "Commitment hash mismatch"
    );

    // Verify track is in registered state
    assert!(track.data.is_registered(), "Track should be in registered state");
    assert!(!track.data.is_certified(), "Track should not be certified yet");
    assert!(!track.data.is_invalidated(), "Track should not be invalidated");

    println!("Track metadata verified successfully!");
    println!("  Key: {:?}...", &key_hash.0[..4]);
    println!("  Size: {} MB", track.size.as_u64());
    println!("  Commitment: {:?}...", &track.data.commitment_hash.0[..4]);
}

/// Test that merkle commitment is stored correctly in track data.
///
/// Verifies the commitment hash field is properly set during registration.
#[tokio::test]
#[ignore]
async fn test_track_commitment() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;

    // Reserve a tape
    let (tape_authority, _) = reserve_test_tape(
        &client,
        &payer,
        StorageUnits(100),
        EpochNumber(1),
        EpochNumber(10),
    )
    .await;

    // Create distinct hashes to verify correct field mapping
    let key_hash = Hash::from([11u8; 32]);
    let root_hash = Hash::from([22u8; 32]);
    let commitment_hash = Hash::from([33u8; 32]);
    let track_size = StorageUnits(25);

    let register_ix = build_register_track_ix(
        payer.pubkey(),
        tape_authority.pubkey(),
        track_size,
        root_hash,
        commitment_hash,
        key_hash,
    );

    client
        .send_instructions_with_signers(&payer, vec![register_ix], &[&tape_authority])
        .await
        .expect("Failed to register track");

    let track = client
        .get_track(&tape_authority.pubkey(), &key_hash)
        .await
        .expect("Failed to fetch track");

    // Verify the commitment hash specifically
    assert_eq!(
        track.data.commitment_hash, commitment_hash,
        "Commitment hash should match the erasure coding commitment"
    );

    // The key hash is used for addressing, commitment is for data integrity
    assert_ne!(
        track.key, track.data.commitment_hash,
        "Key and commitment should be different"
    );

    println!("Track commitment verified!");
    println!("  Key hash: {:?}...", &track.key.0[..4]);
    println!("  Commitment hash: {:?}...", &track.data.commitment_hash.0[..4]);
}

/// Test registering multiple tracks on the same tape.
///
/// Verifies that:
/// 1. Multiple tracks can be added to one tape
/// 2. Each track has a unique ID
/// 3. Tape track count is updated correctly
/// 4. Tape used storage increases appropriately
#[tokio::test]
#[ignore]
async fn test_multiple_tracks_on_tape() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;

    // Reserve a tape with enough capacity for multiple tracks
    let (tape_authority, tape_address) = reserve_test_tape(
        &client,
        &payer,
        StorageUnits(1000), // 1000 MB capacity
        EpochNumber(1),
        EpochNumber(50),
    )
    .await;

    let track_count = 5;
    let track_size = StorageUnits(100); // 100 MB each

    // Register multiple tracks
    let mut track_ids = Vec::new();
    for i in 0..track_count {
        let key_hash = Hash::from([i as u8 + 1; 32]);
        let root_hash = Hash::from([i as u8 + 100; 32]);
        let commitment_hash = Hash::from([i as u8 + 200; 32]);

        let register_ix = build_register_track_ix(
            payer.pubkey(),
            tape_authority.pubkey(),
            track_size,
            root_hash,
            commitment_hash,
            key_hash,
        );

        client
            .send_instructions_with_signers(&payer, vec![register_ix], &[&tape_authority])
            .await
            .expect(&format!("Failed to register track {}", i));

        // Fetch and record track ID
        let track = client
            .get_track(&tape_authority.pubkey(), &key_hash)
            .await
            .expect(&format!("Failed to fetch track {}", i));

        track_ids.push(track.id);

        // Verify track references correct tape
        assert_eq!(
            track.tape, tape_address,
            "Track {} should reference the tape",
            i
        );
    }

    // Verify all track IDs are unique
    let unique_ids: std::collections::HashSet<_> = track_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        track_count,
        "All tracks should have unique IDs"
    );

    // Verify tape track count
    let tape = client
        .get_tape(&tape_authority.pubkey())
        .await
        .expect("Failed to fetch tape");
    assert_eq!(
        tape.track_count, track_count as u64,
        "Tape should have {} tracks",
        track_count
    );

    // Verify tape used storage increased
    let expected_used = StorageUnits(track_count as u64 * track_size.as_u64());
    assert_eq!(
        tape.used, expected_used,
        "Tape used storage should reflect all tracks"
    );

    println!("Multiple tracks test passed!");
    println!("  Tape: {}", tape_address);
    println!("  Tracks registered: {}", track_count);
    println!("  Track IDs: {:?}", track_ids.iter().map(|id| id.as_u64()).collect::<Vec<_>>());
    println!("  Total used: {} MB / {} MB", tape.used.as_u64(), tape.capacity.as_u64());
}
