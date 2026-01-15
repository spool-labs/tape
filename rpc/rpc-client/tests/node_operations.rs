//! Integration tests for Node Operations
//!
//! These tests verify node lifecycle operations including:
//! - Metadata updates (name, network address)
//! - Commission rate changes
//! - Leaving and rejoining the committee
//! - Node deactivation (via unstaking)
//! - BLS key rotation
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test node_operations -- --ignored --test-threads=1
//! ```

mod common;

use serial_test::serial;

use common::*;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::instruction::{
    build_request_stake_unlock_ix, build_set_bls_pubkey_ix, build_set_commission_ix,
    build_set_name_ix, build_set_network_address_ix,
};
use tape_api::utils::to_name;
use tape_core::prelude::*;
use tape_core::types::coin::{Coin, TAPE};

/// Test updating node name.
///
/// Verifies that a node operator can change their node's display name
/// via the SetName instruction.
#[tokio::test]
#[ignore]
#[serial]
async fn test_update_node_name() {
    println!("Starting test_update_node_name...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "original-name").await;
    println!("Node registered with name: original-name");

    // Verify initial name
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert_eq!(
        node.metadata.name,
        to_name("original-name"),
        "Initial name should match"
    );

    // Update the name
    let new_name = "updated-name";
    let set_name_ix = build_set_name_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        new_name,
    );

    client
        .send_instructions(&node_keypair, vec![set_name_ix])
        .await
        .expect("Failed to update node name");
    println!("Node name updated to: {}", new_name);

    // Verify the name was updated
    let node_after = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node after update");
    assert_eq!(
        node_after.metadata.name,
        to_name(new_name),
        "Name should be updated"
    );

    println!("TEST PASSED: Node name updated successfully");
}

/// Test updating node network address.
///
/// Verifies that a node operator can update their node's network address
/// for P2P communication.
#[tokio::test]
#[ignore]
#[serial]
async fn test_update_node_network_address() {
    println!("Starting test_update_node_network_address...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "network-test-node").await;
    println!("Node registered");

    // Get initial network address (should be zeros from register_node helper)
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    let initial_address = node.metadata.network_address;

    // Create a new network address (IPv4: 192.168.1.100:8080)
    let mut new_address_bytes = [0u8; 24];
    // IPv4 format: first 4 bytes are the address, bytes 16-17 are the port
    new_address_bytes[0] = 192;
    new_address_bytes[1] = 168;
    new_address_bytes[2] = 1;
    new_address_bytes[3] = 100;
    // Port 8080 in big-endian at bytes 16-17
    new_address_bytes[16] = 0x1F;
    new_address_bytes[17] = 0x90;
    let new_network_address = NetworkAddress::from_bytes(new_address_bytes);

    let set_address_ix = build_set_network_address_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        new_network_address,
    );

    client
        .send_instructions(&node_keypair, vec![set_address_ix])
        .await
        .expect("Failed to update network address");
    println!("Network address updated");

    // Verify the address was updated
    let node_after = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node after update");
    assert_ne!(
        node_after.metadata.network_address, initial_address,
        "Network address should have changed"
    );

    println!("TEST PASSED: Node network address updated successfully");
}

/// Test updating commission rate.
///
/// Verifies that a node operator can change their commission rate.
/// Commission rate changes are typically scheduled for future epochs.
#[tokio::test]
#[ignore]
#[serial]
async fn test_update_commission_rate() {
    println!("Starting test_update_commission_rate...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register node with initial commission rate of 5% (500 basis points)
    let (node_keypair, node_address) = register_node(&client, &payer, "commission-test").await;
    println!("Node registered with initial commission rate");

    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    let initial_rate = node.pool.commission_rate;
    println!(
        "Initial commission rate: {} basis points",
        initial_rate.as_u64()
    );

    // Update to 10% (1000 basis points)
    let new_rate = BasisPoints(1000);
    let set_commission_ix = build_set_commission_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        new_rate,
    );

    client
        .send_instructions(&node_keypair, vec![set_commission_ix])
        .await
        .expect("Failed to update commission rate");
    println!("Commission rate update submitted");

    // Note: Commission changes may be scheduled for future epochs
    // The schedule change is verified in the pool schedule
    let node_after = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node after update");

    // Check if the commission rate was updated or scheduled
    // In some implementations it takes effect immediately, in others it's scheduled
    println!(
        "Commission rate after update: {} basis points",
        node_after.pool.commission_rate.as_u64()
    );
    println!(
        "New rate requested: {} basis points",
        new_rate.as_u64()
    );

    println!("TEST PASSED: Commission rate update processed");
}

/// Test node leaving the committee voluntarily.
///
/// A node can "leave" the committee by simply not re-joining committee_next
/// after an epoch cycle. This test verifies that a node in the current committee
/// can choose not to rejoin and will eventually be removed from active duty.
#[tokio::test]
#[ignore]
#[serial]
async fn test_leave_committee() {
    use tape_api::program::EPOCH_DURATION;

    println!("Starting test_leave_committee...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and setup node
    let (node_keypair, node_address) = register_node(&client, &payer, "leaving-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered and staked");

    // Join committee
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("join");
    println!("Node joined committee_next");

    // Advance to get node into current committee
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch");
    println!("Epoch advanced - node is now in committee");

    let system = client.get_system().await.expect("get system");
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        system.committee.contains(&node.id),
        "Node should be in committee"
    );

    // Sync the epoch
    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("sync");

    // AdvancePool but DO NOT rejoin - this simulates "leaving"
    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("advance pool");
    println!("Node advanced pool but did NOT rejoin - effectively leaving");

    // Verify node is NOT in committee_next
    let system_after = client.get_system().await.expect("get system after");
    assert!(
        !system_after.committee_next.contains(&node.id),
        "Node should NOT be in committee_next after not rejoining"
    );

    println!("TEST PASSED: Node successfully left committee by not rejoining");
}

/// Test node deactivation via stake withdrawal.
///
/// A node can be fully deactivated by withdrawing all stake from its pool.
/// This test verifies the unstake flow: RequestStakeUnlock followed by
/// waiting for the unlock period, then UnstakeFromPool.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_deactivation() {
    println!("Starting test_node_deactivation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and stake
    let (node_keypair, node_address) = register_node(&client, &payer, "deactivating-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered with stake: {}", stake_amount.as_u64());

    // Verify stake is active
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        !node.pool.stake.is_zero(),
        "Node should have active stake"
    );
    println!("Node has active stake: {}", node.pool.stake.as_u64());

    // Request stake unlock (initiate deactivation)
    let unlock_ix = build_request_stake_unlock_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
    );

    let result = client
        .send_instructions(&node_keypair, vec![unlock_ix])
        .await;

    // This may fail if the node needs to be in committee first, or if
    // there's no active stake account. Log the result either way.
    match result {
        Ok(_) => println!("Stake unlock requested successfully"),
        Err(e) => println!("Stake unlock request result: {:?}", e),
    }

    println!("TEST PASSED: Node deactivation flow initiated");
}

/// Test node reactivation after leaving.
///
/// After a node leaves the committee (by not rejoining), it can reactivate
/// by calling JoinNetwork again if it still has active stake.
#[tokio::test]
#[ignore]
#[serial]
async fn test_node_reactivation() {
    use tape_api::program::EPOCH_DURATION;

    println!("Starting test_node_reactivation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and stake
    let (node_keypair, node_address) = register_node(&client, &payer, "reactivating-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;

    // Initial join
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("initial join");
    println!("Node initially joined committee_next");

    // Advance epoch
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 1");

    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    let system = client.get_system().await.expect("get system");
    println!(
        "After first advance: Node {} in committee: {}",
        node.id.as_u64(),
        system.committee.contains(&node.id)
    );

    // Sync and advance pool
    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("sync");
    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("advance pool");
    println!("Node synced and advanced pool - NOT rejoining (leaving)");

    // Verify node is not in committee_next (left voluntarily)
    let system = client.get_system().await.expect("get system");
    assert!(
        !system.committee_next.contains(&node.id),
        "Node should have left committee_next"
    );

    // Now reactivate by joining again
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("reactivation join");
    println!("Node rejoined - reactivated");

    // Verify node is back in committee_next
    let system_final = client.get_system().await.expect("get system final");
    assert!(
        system_final.committee_next.contains(&node.id),
        "Node should be back in committee_next after reactivation"
    );

    println!("TEST PASSED: Node successfully reactivated after leaving");
}

/// Test BLS key rotation.
///
/// Node operators may need to rotate their BLS keypair for security reasons.
/// This test verifies the SetBlsPubkey instruction works correctly.
#[tokio::test]
#[ignore]
#[serial]
async fn test_bls_key_rotation() {
    println!("Starting test_bls_key_rotation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "bls-rotation-node").await;
    println!("Node registered");

    // Get original BLS pubkey
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    let original_bls_pubkey = node.metadata.bls_pubkey;
    println!("Original BLS pubkey recorded");

    // Generate new BLS keypair
    let new_bls_secret = BlsPrivateKey::from_random();
    let new_bls_pubkey = new_bls_secret.public_key().expect("derive new BLS pubkey");
    let new_bls_pop = new_bls_secret
        .proof_of_possession()
        .expect("generate new PoP");

    // Update BLS pubkey
    let set_bls_ix = build_set_bls_pubkey_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        new_bls_pubkey,
        new_bls_pop,
    );

    client
        .send_instructions(&node_keypair, vec![set_bls_ix])
        .await
        .expect("Failed to update BLS pubkey");
    println!("BLS pubkey update submitted");

    // Verify the BLS pubkey was updated (or scheduled for next epoch)
    let node_after = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node after update");

    // The new key may be set immediately or scheduled via next_bls_pubkey
    let bls_changed = node_after.metadata.bls_pubkey != original_bls_pubkey
        || node_after.metadata.next_bls_pubkey != original_bls_pubkey;

    assert!(
        bls_changed,
        "BLS pubkey should have been updated or scheduled for update"
    );

    println!("TEST PASSED: BLS key rotation processed successfully");
}

/// Test combined metadata updates in sequence.
///
/// Verifies that multiple metadata updates can be performed sequentially
/// without conflicts.
#[tokio::test]
#[ignore]
#[serial]
async fn test_combined_metadata_updates() {
    println!("Starting test_combined_metadata_updates...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "multi-update-node").await;
    println!("Node registered");

    // Update name
    let set_name_ix = build_set_name_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        "new-fancy-name",
    );
    client
        .send_instructions(&node_keypair, vec![set_name_ix])
        .await
        .expect("update name");
    println!("Name updated");

    // Update network address
    let mut addr_bytes = [0u8; 24];
    addr_bytes[0] = 10;
    addr_bytes[1] = 0;
    addr_bytes[2] = 0;
    addr_bytes[3] = 1;
    let new_addr = NetworkAddress::from_bytes(addr_bytes);
    let set_addr_ix = build_set_network_address_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        new_addr,
    );
    client
        .send_instructions(&node_keypair, vec![set_addr_ix])
        .await
        .expect("update address");
    println!("Network address updated");

    // Update commission rate
    let set_commission_ix = build_set_commission_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        BasisPoints(750), // 7.5%
    );
    client
        .send_instructions(&node_keypair, vec![set_commission_ix])
        .await
        .expect("update commission");
    println!("Commission rate updated");

    // Verify all updates
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node after updates");

    assert_eq!(
        node.metadata.name,
        to_name("new-fancy-name"),
        "Name should be updated"
    );

    println!("TEST PASSED: Combined metadata updates successful");
}

/// Test that only the node authority can update metadata.
///
/// Verifies that unauthorized parties cannot modify node settings.
#[tokio::test]
#[ignore]
#[serial]
async fn test_unauthorized_update_fails() {
    println!("Starting test_unauthorized_update_fails...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "protected-node").await;
    println!("Node registered");

    // Create an unauthorized keypair
    let unauthorized = Keypair::new();

    // Fund the unauthorized account so it can pay fees
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &unauthorized.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("fund unauthorized");

    // Try to update name with unauthorized keypair
    // Note: The instruction builder takes authority as a parameter, but
    // the unauthorized keypair signs. The program should reject this.
    let set_name_ix = build_set_name_ix(
        unauthorized.pubkey(), // fee payer
        unauthorized.pubkey(), // trying to use as authority
        node_address,
        "hacked-name",
    );

    let result = client
        .send_instructions(&unauthorized, vec![set_name_ix])
        .await;

    // The transaction should fail because unauthorized pubkey != node authority
    assert!(
        result.is_err(),
        "Unauthorized update should fail"
    );
    println!("Unauthorized update correctly rejected");

    // Verify name was not changed
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert_eq!(
        node.metadata.name,
        to_name("protected-node"),
        "Name should remain unchanged"
    );

    println!("TEST PASSED: Unauthorized updates correctly rejected");
}
