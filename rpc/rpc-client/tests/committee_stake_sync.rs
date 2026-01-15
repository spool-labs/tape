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
//! ```bash
//! cargo test -p rpc-client --test committee_stake_sync -- --ignored --test-threads=1
//! ```

mod common;

use serial_test::serial;

use common::{
    advance_epoch, advance_pool, assert_fsm_action, create_client, debug_state, initialize_system,
    join_committee, register_node, setup_validator, stake_to_node, sync_epoch, transfer_tape,
    ValidatorGuard,
};
use solana_sdk::signature::Signer;
use tape_api::errors::TapeError;
use tape_api::fsm::NodeAction;
use tape_core::types::coin::{Coin, TAPE};

/// Test that committee_next is cleared (not copied) after epoch rotation.
#[tokio::test]
#[ignore]
#[serial]
async fn test_committee_cleared_on_rotation() {
    println!("Starting test_committee_cleared_on_rotation...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node1_keypair, node1_address) = register_node(&client, &payer, "node-1").await;
    let (node2_keypair, node2_address) = register_node(&client, &payer, "node-2").await;
    println!("Nodes registered: {} {}", node1_address, node2_address);

    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node1_keypair.pubkey(), stake_amount.as_u64()).await;
    transfer_tape(&client, &payer, &node2_keypair.pubkey(), stake_amount.as_u64()).await;
    println!("Transferred TAPE to each node");

    stake_to_node(&client, &node1_keypair, node1_address, stake_amount).await;
    stake_to_node(&client, &node2_keypair, node2_address, stake_amount).await;
    println!("Staked {} to each node", stake_amount.as_u64());

    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to join");
    join_committee(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to join");
    println!("Both nodes joined committee_next");

    let system_before = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system_before.committee_next.size(),
        2,
        "committee_next should have 2 members before advance"
    );

    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced");

    let system_after = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system_after.committee_next.size(),
        0,
        "committee_next should be EMPTY after rotation"
    );
    assert_eq!(
        system_after.committee.size(),
        2,
        "committee should have 2 members after advance"
    );

    println!("TEST PASSED: committee_next is cleared on rotation");
}

/// Test that re-joining requires AdvancePool to be called first.
#[tokio::test]
#[ignore]
#[serial]
async fn test_rejoin_requires_advance_pool() {
    println!("Starting test_rejoin_requires_advance_pool...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "test-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered and staked");

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to join");
    println!("Node joined committee_next");

    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");
    println!("Epoch advanced - node is now in committee");

    let system = client.get_system().await.expect("Failed to get system");
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("Failed to get node");
    assert!(
        system.committee.index_of(&node.id).is_some(),
        "Node should be in committee"
    );

    let result = join_committee(&client, &node_keypair, node_address).await;

    assert!(result.is_err(), "JoinNetwork should fail without AdvancePool");
    let err_str = result.unwrap_err();
    // Use typed error parsing
    let tape_err = TapeError::from_error_string(&err_str);
    assert!(
        tape_err == Some(TapeError::NodeStale),
        "Expected NodeStale error, got: {} (parsed: {:?})",
        err_str,
        tape_err
    );

    println!("TEST PASSED: Re-join requires AdvancePool");
}

/// Test that fresh stake from pool.stake is used on re-join.
#[tokio::test]
#[ignore]
#[serial]
async fn test_fresh_stake_on_rejoin() {
    println!("Starting test_fresh_stake_on_rejoin...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "stake-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered with stake: {}", stake_amount.as_u64());

    debug_state(&client, &node_keypair, "Initial: After registration").await;

    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        !node.pool.stake.is_zero(),
        "In low-quorum mode, stake should activate immediately"
    );

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to join");
    println!("Node joined committee_next");

    debug_state(&client, &node_keypair, "After first join").await;

    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");

    debug_state(&client, &node_keypair, "After advance_epoch").await;

    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("Failed to sync epoch");

    debug_state(&client, &node_keypair, "After sync_epoch").await;

    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("Failed to advance pool");
    println!("AdvancePool called");

    debug_state(&client, &node_keypair, "After advance_pool").await;

    let system = client.get_system().await.expect("Failed to get system");
    assert_eq!(
        system.committee_next.size(),
        0,
        "committee_next should be empty after advance_pool"
    );

    let node_after_advance = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("Failed to get node");
    let fresh_stake = node_after_advance.pool.stake;
    assert!(
        !fresh_stake.is_zero(),
        "pool.stake should be non-zero after activation"
    );

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to re-join");
    println!("Node re-joined committee_next");

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

    assert_eq!(
        new_member_stake, fresh_stake,
        "committee_next should use fresh pool.stake"
    );

    println!("TEST PASSED: Fresh stake used on re-join");
}

/// Test that a returning node (not in current committee) uses the new-join path.
#[tokio::test]
#[ignore]
#[serial]
async fn test_returning_node_new_join_path() {
    println!("Starting test_returning_node_new_join_path...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node1_keypair, node1_address) = register_node(&client, &payer, "staying-node").await;
    let (node2_keypair, node2_address) = register_node(&client, &payer, "returning-node").await;

    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node1_keypair.pubkey(), stake_amount.as_u64()).await;
    transfer_tape(&client, &payer, &node2_keypair.pubkey(), stake_amount.as_u64()).await;

    stake_to_node(&client, &node1_keypair, node1_address, stake_amount).await;
    stake_to_node(&client, &node2_keypair, node2_address, stake_amount).await;
    println!("Both nodes registered and staked");

    debug_state(&client, &node1_keypair, "After staking node1").await;
    debug_state(&client, &node2_keypair, "After staking node2").await;

    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to join");
    join_committee(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to join");
    println!("Both nodes joined committee_next");

    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch 1");

    debug_state(&client, &node1_keypair, "After advance_epoch 1").await;

    sync_epoch(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to sync epoch");
    sync_epoch(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to sync epoch");

    debug_state(&client, &node1_keypair, "After sync (both nodes)").await;

    advance_pool(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to advance pool");
    advance_pool(&client, &node2_keypair, node2_address)
        .await
        .expect("Node 2 failed to advance pool");
    println!("AdvancePool called for both nodes");

    debug_state(&client, &node1_keypair, "After advance_pool (both nodes)").await;

    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("Node 1 failed to re-join");
    println!("Node 1 re-joined, Node 2 did NOT re-join");

    debug_state(&client, &node1_keypair, "After node1 re-join").await;

    let system = client.get_system().await.expect("get system");
    let node1_data = client
        .get_node(&node1_keypair.pubkey())
        .await
        .expect("get node1");
    let node2_data = client
        .get_node(&node2_keypair.pubkey())
        .await
        .expect("get node2");

    assert!(
        system.committee.contains(&node1_data.id),
        "Node 1 should be in committee"
    );
    assert!(
        system.committee.contains(&node2_data.id),
        "Node 2 should be in committee"
    );
    assert!(
        system.committee_next.contains(&node1_data.id),
        "Node 1 should be in committee_next"
    );
    assert!(
        !system.committee_next.contains(&node2_data.id),
        "Node 2 should NOT be in committee_next"
    );

    let result = join_committee(&client, &node2_keypair, node2_address).await;

    assert!(
        result.is_ok(),
        "Node 2 should be able to join with active pool.stake: {:?}",
        result.err()
    );
    println!("Node 2 successfully joined");

    let system_final = client.get_system().await.expect("Failed to get system");
    assert!(
        system_final.committee_next.contains(&node1_data.id),
        "Node 1 should be in committee_next"
    );
    assert!(
        system_final.committee_next.contains(&node2_data.id),
        "Node 2 should be in committee_next"
    );

    println!("TEST PASSED: Node uses pool.stake directly on re-join");
}

/// FSM-traced test: Walk through a single epoch cycle step by step.
#[tokio::test]
#[ignore]
#[serial]
async fn test_fsm_single_epoch_flow() {
    use tape_api::fsm::NodeStateMachine;

    println!("Starting test_fsm_single_epoch_flow...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "fsm-test-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Node registered and staked");

    debug_state(&client, &node_keypair, "Step 1: After registration").await;

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to join committee");

    debug_state(&client, &node_keypair, "Step 2: After JoinNetwork").await;

    advance_epoch(&client, &payer)
        .await
        .expect("Failed to advance epoch");

    debug_state(&client, &node_keypair, "Step 3: After AdvanceEpoch").await;

    assert_fsm_action(&client, &node_keypair, NodeAction::SyncEpoch, "After AdvanceEpoch").await;

    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("Failed to sync epoch");

    debug_state(&client, &node_keypair, "Step 4: After SyncEpoch").await;

    let system = Box::new(client.get_system().await.expect("get system"));
    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node"),
    );
    let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);

    if epoch.state.is_active() {
        println!("Bootstrap detected: jumped from Syncing to Active");
        assert_eq!(
            action,
            NodeAction::AdvancePool,
            "Should need AdvancePool in Active phase"
        );
    }

    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("Failed to advance pool");

    debug_state(&client, &node_keypair, "Step 5: After AdvancePool").await;

    assert_fsm_action(&client, &node_keypair, NodeAction::JoinNetwork, "After AdvancePool").await;

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Failed to re-join");

    debug_state(&client, &node_keypair, "Step 6: After re-join").await;

    let system = Box::new(client.get_system().await.expect("get system"));
    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node"),
    );
    let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);

    assert!(
        matches!(
            action,
            NodeAction::WaitForEpochDuration { .. } | NodeAction::AdvanceEpoch
        ),
        "After re-join, should be waiting for epoch or able to advance"
    );

    println!("\nTEST PASSED: FSM single epoch flow verified");
}

/// FSM-traced test: Verify immediate stake activation in low-quorum mode.
#[tokio::test]
#[ignore]
#[serial]
async fn test_fsm_stake_activation_flow() {
    println!("Starting test_fsm_stake_activation_flow...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "stake-test").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;

    debug_state(&client, &node_keypair, "Before staking").await;

    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;

    debug_state(&client, &node_keypair, "After stake deposit").await;

    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");

    assert!(
        !node.pool.stake.is_zero(),
        "In low-quorum mode, stake should activate immediately"
    );
    assert_eq!(
        node.pool.stake.as_u64(),
        stake_amount.as_u64(),
        "pool.stake should equal the staked amount immediately"
    );

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("Should be able to join with immediately active stake");

    debug_state(&client, &node_keypair, "After join").await;

    let system = client.get_system().await.expect("get system");
    assert!(
        system.committee_next.contains(&node.id),
        "Node should be in committee_next"
    );

    let member = system
        .committee_next
        .member_at(0)
        .expect("Should have member");
    assert_eq!(
        member.stake.as_u64(),
        stake_amount.as_u64(),
        "committee_next should reflect immediately active stake"
    );

    println!("\nTEST PASSED: Immediate stake activation verified");
}
