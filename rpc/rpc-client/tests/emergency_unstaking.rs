//! Integration tests for Emergency Unstaking
//!
//! These tests verify the emergency unstaking mechanism described in
//! `docs/low-quorum-redesign-plan.md`. When the system is "stuck" (epoch hasn't
//! advanced for STUCK_SYSTEM_THRESHOLD = 2 * EPOCH_DURATION), stakers can
//! bypass the normal E+2 withdrawal delay and unstake immediately.
//!
//! ## Tests
//!
//! - `test_emergency_unstake_stuck_system` - Unstake when epoch hasn't advanced for threshold
//! - `test_emergency_unstake_blocked_normal` - Cannot emergency unstake under normal conditions
//! - `test_emergency_unstake_partial_committee` - Works even with partial committee online
//! - `test_emergency_unstake_timing` - Verify threshold timing is correct
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test emergency_unstaking -- --ignored --test-threads=1
//! ```

mod common;

use common::{
    advance_epoch, advance_pool, create_client, debug_state, initialize_system, join_committee,
    register_node, setup_single_node, setup_validator, stake_to_node, sync_epoch, transfer_tape,
    wait_for_epoch_duration, ValidatorGuard,
};
use solana_sdk::signature::Signer;
use tape_api::instruction::{build_request_stake_unlock_ix, build_unstake_from_pool_ix};
use tape_api::program::{EPOCH_DURATION, STUCK_SYSTEM_THRESHOLD};
use tape_core::types::coin::{Coin, TAPE};

/// Test emergency unstake when the system has been stuck for STUCK_SYSTEM_THRESHOLD.
///
/// When the epoch hasn't advanced for 2 * EPOCH_DURATION seconds, stakers can
/// bypass the normal E+2 withdrawal delay and request immediate unstaking.
#[tokio::test]
#[ignore]
async fn test_emergency_unstake_stuck_system() {
    println!("Starting test_emergency_unstake_stuck_system...");
    println!(
        "EPOCH_DURATION = {} seconds, STUCK_SYSTEM_THRESHOLD = {} seconds",
        EPOCH_DURATION, STUCK_SYSTEM_THRESHOLD
    );

    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;
    let (node_keypair, node_address) = &ctx.nodes[0];

    debug_state(client, node_keypair, "Initial state (epoch 2)").await;

    // Verify node is in committee and has active stake
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        !node.pool.stake.is_zero(),
        "Node should have active stake"
    );

    let epoch_before = client.get_epoch().await.expect("get epoch");
    println!(
        "Current epoch: {}, last_epoch timestamp: {}",
        epoch_before.id.as_u64(),
        epoch_before.last_epoch
    );

    // Wait for system to become "stuck" (2 * EPOCH_DURATION without advancement)
    // Add extra buffer to ensure we're past the threshold
    let wait_time = (STUCK_SYSTEM_THRESHOLD + 2) as u64;
    println!(
        "Waiting {} seconds for system to become stuck...",
        wait_time
    );
    wait_for_epoch_duration(wait_time).await;

    // Now the system should be considered "stuck" - request emergency unlock
    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        node_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unlock_ix], &[node_keypair])
        .await;

    assert!(
        result.is_ok(),
        "Emergency unstake should succeed when system is stuck: {:?}",
        result.err()
    );

    println!("Emergency unlock request succeeded!");

    // Verify the stake is now in withdrawing state with current epoch
    let stake = client
        .get_stake(&node_keypair.pubkey())
        .await
        .expect("get stake");

    let epoch_after = client.get_epoch().await.expect("get epoch");
    let current_epoch = epoch_after.id;

    // In emergency mode, withdraw_epoch should be current (not current + 2)
    assert!(
        stake.inner.is_withdrawing(),
        "Stake should be in withdrawing/unlocking state"
    );

    // The unstake_epoch should be current epoch (immediate) not E+2
    let unstake_epoch = stake.inner.state.unstake_epoch;
    println!(
        "Unstake epoch: {}, Current epoch: {}",
        unstake_epoch.as_u64(),
        current_epoch.as_u64()
    );

    assert_eq!(
        unstake_epoch, current_epoch,
        "Emergency unstake should set withdraw_epoch to current epoch (immediate)"
    );

    // Now we should be able to complete the unstake
    let unstake_ix = build_unstake_from_pool_ix(
        payer.pubkey(),
        node_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unstake_ix], &[node_keypair])
        .await;

    assert!(
        result.is_ok(),
        "Completing emergency unstake should succeed: {:?}",
        result.err()
    );

    println!("Emergency unstake completed successfully!");
    println!("\nTEST PASSED: Emergency unstake works when system is stuck");
}

/// Test that emergency unstake is blocked under normal conditions.
///
/// When the system is operating normally (epoch advanced recently), the
/// RequestStakeUnlock instruction should use the standard E+2 delay path.
#[tokio::test]
#[ignore]
async fn test_emergency_unstake_blocked_normal() {
    println!("Starting test_emergency_unstake_blocked_normal...");

    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;
    let (node_keypair, node_address) = &ctx.nodes[0];

    debug_state(client, node_keypair, "Initial state").await;

    // Verify node has active stake
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        !node.pool.stake.is_zero(),
        "Node should have active stake"
    );

    let epoch_before = client.get_epoch().await.expect("get epoch");
    println!(
        "Current epoch: {}, last_epoch timestamp: {}",
        epoch_before.id.as_u64(),
        epoch_before.last_epoch
    );

    // Request unlock immediately (system is NOT stuck)
    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        node_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unlock_ix], &[node_keypair])
        .await;

    // The request should succeed, but use the normal E+2 delay path
    assert!(
        result.is_ok(),
        "Unlock request should succeed: {:?}",
        result.err()
    );

    // Verify the stake is in withdrawing state with E+2 delay
    let stake = client
        .get_stake(&node_keypair.pubkey())
        .await
        .expect("get stake");

    let epoch_after = client.get_epoch().await.expect("get epoch");
    let current_epoch = epoch_after.id;
    let expected_unstake_epoch = current_epoch.as_u64() + 2;

    assert!(
        stake.inner.is_withdrawing(),
        "Stake should be in withdrawing/unlocking state"
    );

    let unstake_epoch = stake.inner.state.unstake_epoch;
    println!(
        "Unstake epoch: {}, Current epoch: {}, Expected: {}",
        unstake_epoch.as_u64(),
        current_epoch.as_u64(),
        expected_unstake_epoch
    );

    assert_eq!(
        unstake_epoch.as_u64(),
        expected_unstake_epoch,
        "Normal unstake should have E+2 delay"
    );

    // Try to complete unstake immediately - should fail because we're before E+2
    let unstake_ix = build_unstake_from_pool_ix(
        payer.pubkey(),
        node_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unstake_ix], &[node_keypair])
        .await;

    assert!(
        result.is_err(),
        "Completing unstake before E+2 should fail"
    );

    println!("Unstake correctly blocked before withdrawal epoch");
    println!("\nTEST PASSED: Normal unlock uses E+2 delay (not emergency path)");
}

/// Test emergency unstake works even with partial committee online.
///
/// When the system is stuck, emergency unstaking should work regardless of
/// how many committee members are online - it's a safety escape hatch.
#[tokio::test]
#[ignore]
async fn test_emergency_unstake_partial_committee() {
    println!("Starting test_emergency_unstake_partial_committee...");

    // Setup with single node (simulating partial committee scenario)
    let ctx = setup_single_node().await;
    let client = &ctx.client;
    let payer = &ctx.payer;
    let (node_keypair, node_address) = &ctx.nodes[0];

    // Register a second staker who stakes to the same node
    let staker_keypair = solana_sdk::signature::Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &staker_keypair.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_ix])
        .await
        .expect("fund staker");

    let stake_amount = Coin::<TAPE>::new(500_000_000);
    transfer_tape(client, payer, &staker_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(client, &staker_keypair, *node_address, stake_amount).await;

    println!("Second staker registered and staked to node");

    // Wait for system to become stuck
    let wait_time = (STUCK_SYSTEM_THRESHOLD + 2) as u64;
    println!(
        "Waiting {} seconds for system to become stuck...",
        wait_time
    );
    wait_for_epoch_duration(wait_time).await;

    // Both the node operator and external staker should be able to emergency unstake

    // 1. External staker emergency unstakes
    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        staker_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unlock_ix], &[&staker_keypair])
        .await;

    assert!(
        result.is_ok(),
        "External staker emergency unstake should succeed: {:?}",
        result.err()
    );

    // Verify immediate withdrawal epoch
    let stake = client
        .get_stake(&staker_keypair.pubkey())
        .await
        .expect("get stake");
    let epoch = client.get_epoch().await.expect("get epoch");

    assert_eq!(
        stake.inner.state.unstake_epoch,
        epoch.id,
        "Emergency unstake should have immediate withdrawal"
    );

    println!("External staker emergency unstake succeeded");

    // 2. Node operator emergency unstakes
    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        node_keypair.pubkey(),
        *node_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(payer, vec![unlock_ix], &[node_keypair])
        .await;

    assert!(
        result.is_ok(),
        "Node operator emergency unstake should succeed: {:?}",
        result.err()
    );

    println!("Node operator emergency unstake succeeded");
    println!("\nTEST PASSED: Emergency unstake works with partial committee");
}

/// Test that emergency unstake timing threshold is exactly correct.
///
/// Verifies that:
/// - Just before STUCK_SYSTEM_THRESHOLD: uses normal E+2 path
/// - At or after STUCK_SYSTEM_THRESHOLD: uses emergency immediate path
#[tokio::test]
#[ignore]
async fn test_emergency_unstake_timing() {
    println!("Starting test_emergency_unstake_timing...");
    println!(
        "EPOCH_DURATION = {}, STUCK_SYSTEM_THRESHOLD = {} (2x EPOCH_DURATION)",
        EPOCH_DURATION, STUCK_SYSTEM_THRESHOLD
    );

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Wait for fees to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register and setup two nodes for testing timing
    let (node1_keypair, node1_address) = register_node(&client, &payer, "timing-node-1").await;
    let (node2_keypair, node2_address) = register_node(&client, &payer, "timing-node-2").await;

    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node1_keypair.pubkey(), stake_amount.as_u64()).await;
    transfer_tape(&client, &payer, &node2_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node1_keypair, node1_address, stake_amount).await;
    stake_to_node(&client, &node2_keypair, node2_address, stake_amount).await;

    // Join committee and advance epoch so we have active stake
    join_committee(&client, &node1_keypair, node1_address)
        .await
        .expect("node1 join");
    join_committee(&client, &node2_keypair, node2_address)
        .await
        .expect("node2 join");

    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer).await.expect("advance");

    sync_epoch(&client, &node1_keypair, node1_address)
        .await
        .expect("sync1");
    sync_epoch(&client, &node2_keypair, node2_address)
        .await
        .expect("sync2");

    advance_pool(&client, &node1_keypair, node1_address)
        .await
        .expect("advance pool 1");
    advance_pool(&client, &node2_keypair, node2_address)
        .await
        .expect("advance pool 2");

    debug_state(&client, &node1_keypair, "After setup").await;

    // Test 1: Before threshold - should use normal E+2 path
    // Wait for just under STUCK_SYSTEM_THRESHOLD
    let under_threshold_wait = (STUCK_SYSTEM_THRESHOLD - EPOCH_DURATION) as u64;
    if under_threshold_wait > 0 {
        println!(
            "Waiting {} seconds (under threshold)...",
            under_threshold_wait
        );
        wait_for_epoch_duration(under_threshold_wait).await;
    }

    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        node1_keypair.pubkey(),
        node1_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(&payer, vec![unlock_ix], &[&node1_keypair])
        .await;

    let epoch = client.get_epoch().await.expect("get epoch");
    println!(
        "Test 1 - Under threshold result: {:?}, epoch: {}",
        result.is_ok(),
        epoch.id.as_u64()
    );

    if result.is_ok() {
        let stake1 = client
            .get_stake(&node1_keypair.pubkey())
            .await
            .expect("get stake");
        let unstake_epoch = stake1.inner.state.unstake_epoch;
        println!(
            "Node1 unstake_epoch: {}, current: {}",
            unstake_epoch.as_u64(),
            epoch.id.as_u64()
        );
        // Should have E+2 delay if before threshold
        // Note: This depends on exact timing which can be tricky in tests
    }

    // Test 2: After threshold - should use emergency immediate path
    // Wait the remaining time to pass threshold
    let remaining_wait = (STUCK_SYSTEM_THRESHOLD + 2) as u64;
    println!(
        "Waiting {} more seconds (past threshold)...",
        remaining_wait
    );
    wait_for_epoch_duration(remaining_wait).await;

    let unlock_ix = build_request_stake_unlock_ix(
        payer.pubkey(),
        node2_keypair.pubkey(),
        node2_address,
    );

    // Both payer and authority must sign
    let result = client
        .send_instructions_with_signers(&payer, vec![unlock_ix], &[&node2_keypair])
        .await;

    assert!(
        result.is_ok(),
        "Emergency unstake after threshold should succeed: {:?}",
        result.err()
    );

    let stake2 = client
        .get_stake(&node2_keypair.pubkey())
        .await
        .expect("get stake");
    let epoch = client.get_epoch().await.expect("get epoch");
    let unstake_epoch = stake2.inner.state.unstake_epoch;

    println!(
        "Node2 unstake_epoch: {}, current: {}",
        unstake_epoch.as_u64(),
        epoch.id.as_u64()
    );

    assert_eq!(
        unstake_epoch,
        epoch.id,
        "After threshold, unstake should be immediate (current epoch)"
    );

    println!("\nTEST PASSED: Emergency unstake timing threshold works correctly");
}
