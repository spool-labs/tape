//! E2E tests for Committee Stake Synchronization
//!
//! These tests verify the committee stake synchronization mechanism described in
//! `docs/committee-stake-synchronization.md`. They test:
//!
//! 1. Committee rotation clears `committee_next` (not copy)
//! 2. Re-joining nodes must call AdvancePool first (NodeStale error otherwise)
//! 3. Fresh stake from `pool.stake` is used on re-join
//! 4. Returning nodes (not in current committee) use new-join path
//!
//! ## Running Tests
//!
//! These tests require the Solana programs to be built first:
//! ```bash
//! make build
//! ```
//!
//! Run individual tests (recommended due to memory requirements):
//! ```bash
//! cargo test -p rpc-client --test committee_stake_sync test_committee_cleared_on_rotation -- --ignored
//! cargo test -p rpc-client --test committee_stake_sync test_rejoin_requires_advance_pool -- --ignored
//! cargo test -p rpc-client --test committee_stake_sync test_fresh_stake_on_rejoin -- --ignored
//! cargo test -p rpc-client --test committee_stake_sync test_returning_node_new_join_path -- --ignored
//! ```
//!
//! Run all tests (requires 8GB+ RAM):
//! ```bash
//! cargo test -p rpc-client --test committee_stake_sync -- --ignored --test-threads=1
//! ```

mod common;

use common::{create_client, initialize_system, setup_validator, ValidatorGuard};
use rpc_client::RpcClient;
use rpc_test::TestRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use tape_api::instruction::{
    build_advance_epoch_ix, build_advance_pool_ix, build_join_network_ix, build_register_node_ix,
    build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::prelude::*;

// =============================================================================
// Test-Specific Helpers
// =============================================================================

/// Register a node and return its keypair and node address
async fn register_node(
    client: &RpcClient<TestRpc>,
    payer: &Keypair,
    name: &str,
) -> (Keypair, Pubkey) {
    let node_keypair = Keypair::new();
    let (node_address, _) = node_pda(node_keypair.pubkey());

    // Fund the node authority
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &node_keypair.pubkey(),
        1_000_000_000, // 1 SOL
    );
    client
        .send_instructions(payer, vec![transfer_ix])
        .await
        .expect("Failed to fund node");

    // Generate BLS keypair
    let bls_secret = BlsPrivateKey::from_random();
    let bls_pubkey = bls_secret.public_key().expect("derive BLS pubkey");
    let bls_pop = bls_secret.proof_of_possession().expect("generate PoP");

    // Register the node
    let register_ix = build_register_node_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        to_name(name),
        BasisPoints(500), // 5% commission
        NetworkAddress::from_bytes([0u8; 24]),
        Pubkey::new_unique(), // TLS pubkey
        bls_pubkey,
        bls_pop,
    );

    client
        .send_instructions(&node_keypair, vec![register_ix])
        .await
        .expect("Failed to register node");

    (node_keypair, node_address)
}

/// Stake TAPE tokens to a node's pool using a specific staker
/// The staker must have TAPE tokens in their ATA
async fn stake_to_node(
    client: &RpcClient<TestRpc>,
    staker: &Keypair,
    node_address: Pubkey,
    amount: Coin<TAPE>,
) {
    let stake_ix = build_stake_with_pool_ix(staker.pubkey(), staker.pubkey(), node_address, amount);

    client
        .send_instructions(staker, vec![stake_ix])
        .await
        .expect("Failed to stake to node");
}

/// Transfer TAPE tokens from payer to recipient
async fn transfer_tape(
    client: &RpcClient<TestRpc>,
    payer: &Keypair,
    recipient: &Pubkey,
    amount: u64,
) {
    use tape_api::program::token::mint_pda;
    use tape_api::utils::ata;

    let (mint_address, _) = mint_pda();
    let source_ata = ata(&payer.pubkey());
    let dest_ata = ata(recipient);

    // Create recipient's ATA if needed
    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &payer.pubkey(),
            recipient,
            &mint_address,
            &spl_token::id(),
        );

    // Transfer tokens
    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &source_ata,
        &dest_ata,
        &payer.pubkey(),
        &[],
        amount,
    )
    .expect("Failed to create transfer instruction");

    client
        .send_instructions(payer, vec![create_ata_ix, transfer_ix])
        .await
        .expect("Failed to transfer TAPE tokens");
}

/// Join a node to the committee
async fn join_committee(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<(), String> {
    let join_ix = build_join_network_ix(node_keypair.pubkey(), node_keypair.pubkey(), node_address);

    client
        .send_instructions(node_keypair, vec![join_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Advance the epoch
async fn advance_epoch(client: &RpcClient<TestRpc>, payer: &Keypair) -> Result<(), String> {
    let advance_ix = build_advance_epoch_ix(payer.pubkey(), payer.pubkey());

    client
        .send_instructions(payer, vec![advance_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Call AdvancePool for a node
async fn advance_pool(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<(), String> {
    let advance_ix =
        build_advance_pool_ix(node_keypair.pubkey(), node_keypair.pubkey(), node_address);

    client
        .send_instructions(node_keypair, vec![advance_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Advance multiple epochs, keeping nodes in committee each time.
/// This ensures stake becomes active (stake activates at E+2).
/// Returns after `count` epoch advances.
async fn advance_epochs_with_nodes(
    client: &RpcClient<TestRpc>,
    payer: &Keypair,
    nodes: &[(&Keypair, Pubkey)],
    count: usize,
) {
    for i in 0..count {
        // Each node must advance pool then re-join to stay in committee
        for (node_keypair, node_address) in nodes {
            let _ = advance_pool(client, node_keypair, *node_address).await;
            let _ = join_committee(client, node_keypair, *node_address).await;
        }
        let _ = advance_epoch(client, payer).await;
        println!("Epoch {} advanced", i + 1);
    }
}

// =============================================================================
// E2E Tests
// =============================================================================

/// Test that committee_next is cleared (not copied) after epoch rotation.
///
/// Flow:
/// 1. Register and stake 2 nodes
/// 2. Both nodes join committee_next
/// 3. Advance epoch (moves nodes from committee_next to committee)
/// 4. Verify committee_next is now EMPTY
#[tokio::test]
#[ignore]
async fn test_committee_cleared_on_rotation() {
    println!("Starting test_committee_cleared_on_rotation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize system
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register two nodes
    let (node1_keypair, node1_address) = register_node(&client, &payer, "node-1").await;
    let (node2_keypair, node2_address) = register_node(&client, &payer, "node-2").await;
    println!("Nodes registered: {} {}", node1_address, node2_address);

    // Transfer TAPE to each node so they can stake
    let stake_amount = Coin::<TAPE>::new(1_000_000_000); // 1000 TAPE (6 decimals)
    transfer_tape(&client, &payer, &node1_keypair.pubkey(), stake_amount.as_u64()).await;
    transfer_tape(&client, &payer, &node2_keypair.pubkey(), stake_amount.as_u64()).await;
    println!("Transferred TAPE to each node");

    // Each node stakes to its own pool
    stake_to_node(&client, &node1_keypair, node1_address, stake_amount).await;
    stake_to_node(&client, &node2_keypair, node2_address, stake_amount).await;
    println!("Staked {} to each node", stake_amount.as_u64());

    // Both nodes join committee
    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to join");
    join_committee(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to join");
    println!("Both nodes joined committee_next");

    // Verify committee_next has 2 members before advance
    let system_before = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system_before.committee_next.size(),
        2,
        "committee_next should have 2 members before advance"
    );
    println!(
        "committee_next size before advance: {}",
        system_before.committee_next.size()
    );

    // Advance epoch (this should move nodes to committee and CLEAR committee_next)
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced");

    // Verify committee_next is now EMPTY
    let system_after = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system_after.committee_next.size(),
        0,
        "committee_next should be EMPTY after rotation (not copied from committee)"
    );
    println!(
        "committee_next size after advance: {} (expected 0)",
        system_after.committee_next.size()
    );

    // Verify committee now has 2 members
    assert_eq!(
        system_after.committee.size(),
        2,
        "committee should have 2 members after advance"
    );
    println!(
        "committee size after advance: {}",
        system_after.committee.size()
    );

    println!("TEST PASSED: committee_next is cleared on rotation");
}

/// Test that re-joining requires AdvancePool to be called first.
///
/// Flow:
/// 1. Setup node in committee
/// 2. Advance epoch (node is now in committee, committee_next is empty)
/// 3. Try to join without calling AdvancePool first
/// 4. Expect NodeStale (0x60) error
#[tokio::test]
#[ignore]
async fn test_rejoin_requires_advance_pool() {
    println!("Starting test_rejoin_requires_advance_pool...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize system
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and stake a node
    let (node_keypair, node_address) = register_node(&client, &payer, "test-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered and staked");

    // Node joins committee_next
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to join");
    println!("Node joined committee_next");

    // Advance epoch - node moves to committee, committee_next is cleared
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced - node is now in committee");

    // Verify node is in committee
    let system = client.get_system().await.expect("Failed to get system");
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("Failed to get node");
    assert!(
        system.committee.index_of(&node.id).is_some(),
        "Node should be in committee"
    );
    println!("Verified: node is in committee");

    // Try to re-join WITHOUT calling AdvancePool first
    // This should fail with NodeStale error
    let result = join_committee(&client, &node_keypair, node_address).await;

    assert!(result.is_err(), "JoinNetwork should fail without AdvancePool");
    let err_str = result.unwrap_err();
    assert!(
        err_str.contains("0x60") || err_str.contains("NodeStale"),
        "Expected NodeStale (0x60) error, got: {}",
        err_str
    );
    println!("Got expected error: NodeStale");

    println!("TEST PASSED: Re-join requires AdvancePool");
}

/// Test that fresh stake from pool.stake is used on re-join.
///
/// This test verifies that when a node re-joins the committee after being active,
/// it uses the current pool.stake value (set by AdvancePool) rather than stale data.
///
/// Flow:
/// 1. Setup node with stake, advance enough epochs for stake to activate
/// 2. Node is in committee with active stake
/// 3. Advance epoch (committee_next is cleared)
/// 4. Node calls AdvancePool (updates latest_advance_epoch)
/// 5. Node re-joins committee_next
/// 6. Verify committee_next has node with correct stake from pool.stake
#[tokio::test]
#[ignore]
async fn test_fresh_stake_on_rejoin() {
    println!("Starting test_fresh_stake_on_rejoin...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize system
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and stake a node
    let (node_keypair, node_address) = register_node(&client, &payer, "stake-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000); // 1000 TAPE
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered with stake: {}", stake_amount.as_u64());

    // Node joins committee_next (in low-quorum mode, uses pending stake)
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to join");
    println!("Node joined committee_next");

    // Advance multiple epochs so stake becomes active (stake activates at E+2)
    // Need to keep node in committee through each epoch
    let nodes = vec![(&node_keypair, node_address)];

    // First advance - node moves to committee
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch 1");
    println!("Epoch 1 advanced - node in committee");

    // Subsequent advances to activate stake
    advance_epochs_with_nodes(&client, &payer, &nodes, 2).await;
    println!("Stake should now be active");

    // Now test the re-join flow
    // Advance epoch again (committee_next is cleared)
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced - committee_next is now empty");

    // Verify committee_next is empty
    let system = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system.committee_next.size(),
        0,
        "committee_next should be empty"
    );

    // Node calls AdvancePool (required before re-join)
    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("Failed to advance pool");
    println!("AdvancePool called");

    // Get fresh stake after AdvancePool
    let node_after_advance = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("Failed to get node");
    let fresh_stake = node_after_advance.pool.stake;
    println!(
        "Fresh pool.stake after AdvancePool: {}",
        fresh_stake.as_u64()
    );

    // Verify stake is non-zero (it should be active now)
    assert!(
        !fresh_stake.is_zero(),
        "pool.stake should be non-zero after activation"
    );

    // Node re-joins committee_next
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to re-join");
    println!("Node re-joined committee_next");

    // Verify committee_next has node with fresh stake
    let system_final = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system_final.committee_next.size(),
        1,
        "committee_next should have 1 member"
    );

    let new_member_stake = system_final
        .committee_next
        .member_at(0)
        .expect("Member should exist at index 0")
        .stake;
    println!(
        "Stake in committee_next: {} (expected: {})",
        new_member_stake.as_u64(),
        fresh_stake.as_u64()
    );

    // The new stake should equal the fresh pool.stake
    assert_eq!(
        new_member_stake, fresh_stake,
        "committee_next should use fresh pool.stake"
    );

    println!("TEST PASSED: Fresh stake used on re-join");
}

/// Test that a returning node (not in current committee) uses the new-join path.
///
/// Flow:
/// 1. Setup two nodes in committee, advance enough epochs for stake to activate
/// 2. Advance epoch without node2 re-joining (node2 falls out of committee)
/// 3. Node2 tries to join (should succeed via new-join path, no AdvancePool needed)
#[tokio::test]
#[ignore]
async fn test_returning_node_new_join_path() {
    println!("Starting test_returning_node_new_join_path...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize system
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and stake two nodes
    let (node1_keypair, node1_address) = register_node(&client, &payer, "staying-node").await;
    let (node2_keypair, node2_address) = register_node(&client, &payer, "returning-node").await;

    // Transfer TAPE to each node so they can stake
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node1_keypair.pubkey(), stake_amount.as_u64()).await;
    transfer_tape(&client, &payer, &node2_keypair.pubkey(), stake_amount.as_u64()).await;

    // Each node stakes to its own pool
    stake_to_node(&client, &node1_keypair, node1_address, stake_amount).await;
    stake_to_node(&client, &node2_keypair, node2_address, stake_amount).await;
    println!("Both nodes registered and staked");

    // Both nodes join committee_next
    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to join");
    join_committee(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to join");
    println!("Both nodes joined committee_next");

    // First advance - both nodes move to committee
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch 1");
    println!("Epoch 1 advanced - both nodes in committee");

    // Advance multiple epochs to activate stake (stake activates at E+2)
    // Keep both nodes in committee during stake activation
    let both_nodes = vec![(&node1_keypair, node1_address), (&node2_keypair, node2_address)];
    advance_epochs_with_nodes(&client, &payer, &both_nodes, 2).await;
    println!("Stake activated for both nodes");

    // Now only node1 re-joins (node2 will fall out)
    advance_pool(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to advance pool");
    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to re-join");
    println!("Node 1 re-joined, Node 2 did NOT re-join");

    // Advance epoch - node1 stays in committee, node2 falls out
    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced - node2 should have fallen out");

    // Verify node2 is NOT in current committee (it's a returning node now)
    let system = client.get_system().await.expect("Failed to get system");
    let node2_data = client
        .get_node(&node2_keypair.pubkey())
        .await
        .expect("Failed to get node 2");

    let node2_in_committee = system.committee.index_of(&node2_data.id).is_some();
    assert!(
        !node2_in_committee,
        "Node 2 should NOT be in current committee"
    );
    println!("Verified: Node 2 is not in current committee (returning node)");

    // Node 2 tries to join WITHOUT calling AdvancePool
    // This should SUCCEED because it's NOT in current committee (new-join path)
    // In low-quorum mode, new-join uses calculate_total_pending_stake()
    let result = join_committee(&client, &node2_keypair, node2_address).await;

    assert!(
        result.is_ok(),
        "Returning node should be able to join without AdvancePool: {:?}",
        result.err()
    );
    println!("Node 2 successfully joined via new-join path");

    // Verify node2 is in committee_next
    let system_final = client.get_system().await.expect("Failed to get system");
    let node2_in_next = system_final.committee_next.contains(&node2_data.id);
    assert!(node2_in_next, "Node 2 should be in committee_next");

    println!("TEST PASSED: Returning node uses new-join path");
}
