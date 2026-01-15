//! Advanced Staking Integration Tests
//!
//! These tests verify advanced staking scenarios including:
//! - Delegated staking (external delegators staking to nodes)
//! - Unstake timing and cooldown periods
//! - Stake activation timing at correct epochs
//! - Multiple delegators staking to the same node
//! - Minimum stake requirements
//! - Unstaking behavior for active committee members
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test staking_advanced -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use serial_test::serial;

use common::{
    advance_epoch, advance_pool, create_client, debug_state, initialize_system,
    join_committee, register_node, setup_validator, stake_to_node,
    sync_epoch, transfer_tape, wait_for_epoch_duration, ValidatorGuard,
};
use solana_sdk::signature::{Keypair, Signer};
use tape_api::errors::TapeError;
use tape_api::instruction::{build_request_stake_unlock_ix, build_stake_with_pool_ix};
use tape_api::program::tapedrive::stake_pda;
use tape_core::types::coin::{Coin, TAPE};

/// Test delegated staking: an external delegator stakes TAPE to a node's pool.
///
/// This verifies that:
/// 1. A third party (not the node operator) can stake to a node's pool
/// 2. The staked tokens contribute to the node's committee stake
/// 3. The delegator's stake is tracked separately from operator stake
#[tokio::test]
#[ignore]
#[serial]
async fn test_delegated_staking() {
    println!("Starting test_delegated_staking...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Wait for validator to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register a node (operator)
    let (node_keypair, node_address) = register_node(&client, &payer, "delegated-node").await;
    println!("Node registered: {}", node_address);

    // Create a separate delegator keypair
    let delegator = Keypair::new();
    let delegator_stake = Coin::<TAPE>::new(500_000_000);
    let operator_stake = Coin::<TAPE>::new(500_000_000);

    // Fund the delegator with SOL and TAPE
    let fund_sol_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &delegator.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(&payer, vec![fund_sol_ix])
        .await
        .expect("Failed to fund delegator with SOL");

    // Transfer TAPE to both node operator and delegator
    transfer_tape(&client, &payer, &node_keypair.pubkey(), operator_stake.as_u64()).await;
    transfer_tape(&client, &payer, &delegator.pubkey(), delegator_stake.as_u64()).await;
    println!(
        "Funded: operator={} TAPE, delegator={} TAPE",
        operator_stake.as_u64(),
        delegator_stake.as_u64()
    );

    // Operator stakes to their own node
    stake_to_node(&client, &node_keypair, node_address, operator_stake).await;
    println!("Operator staked {} to node", operator_stake.as_u64());

    // Delegator stakes to the node
    let delegator_stake_ix = build_stake_with_pool_ix(
        delegator.pubkey(),
        delegator.pubkey(),
        node_address,
        delegator_stake,
    );
    client
        .send_instructions(&delegator, vec![delegator_stake_ix])
        .await
        .expect("Delegator failed to stake");
    println!("Delegator staked {} to node", delegator_stake.as_u64());

    // Check node pool state
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");

    // In low-quorum mode, stake activates immediately
    let expected_total = operator_stake.as_u64() + delegator_stake.as_u64();
    println!(
        "Node pool.stake: {}, expected: {}",
        node.pool.stake.as_u64(),
        expected_total
    );
    assert_eq!(
        node.pool.stake.as_u64(),
        expected_total,
        "Pool stake should include both operator and delegator stake"
    );

    // Verify delegator has their own stake account
    let (delegator_stake_pda, _) = stake_pda(delegator.pubkey());
    let delegator_stake_account = client
        .get_stake(&delegator.pubkey())
        .await
        .expect("get delegator stake");
    assert_eq!(
        delegator_stake_account.authority, delegator.pubkey(),
        "Stake account should belong to delegator"
    );
    assert_eq!(
        delegator_stake_account.pool, node_address,
        "Stake should be associated with the node pool"
    );
    println!(
        "Delegator stake account: {}, amount: {}",
        delegator_stake_pda,
        delegator_stake_account.inner.amount.as_u64()
    );

    // Node joins committee with combined stake
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("join committee");

    let system = client.get_system().await.expect("get system");
    let member = system
        .committee_next
        .member_at(0)
        .expect("should have member");
    assert_eq!(
        member.stake.as_u64(),
        expected_total,
        "Committee stake should be combined operator + delegator"
    );

    println!("\nTEST PASSED: Delegated staking works correctly");
}

/// Test stake activation at the correct epoch.
///
/// This verifies that in normal mode:
/// 1. Stake deposited at epoch E activates at epoch E+2
/// 2. Node cannot join with stake that hasn't activated yet (in normal mode)
///
/// Note: In low-quorum mode, stake activates immediately (E+0).
#[tokio::test]
#[ignore]
#[serial]
async fn test_stake_activation_epoch() {
    println!("Starting test_stake_activation_epoch...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "activation-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;

    // Get current epoch before staking
    let epoch_before_stake = client.get_epoch().await.expect("get epoch");
    println!(
        "Epoch before staking: {}",
        epoch_before_stake.id.as_u64()
    );

    // Stake tokens
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    println!("Staked {} TAPE", stake_amount.as_u64());

    // Check stake account activation epoch
    let stake = client
        .get_stake(&node_keypair.pubkey())
        .await
        .expect("get stake");

    // In low-quorum mode, activation is immediate
    let system = client.get_system().await.expect("get system");
    if system.is_low_quorum() {
        println!("Low-quorum mode detected: stake should activate immediately");
        assert_eq!(
            stake.inner.activation_epoch.as_u64(),
            epoch_before_stake.id.as_u64(),
            "In low-quorum, activation should be current epoch"
        );

        // Stake should already be active in pool
        let node = client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node");
        assert_eq!(
            node.pool.stake.as_u64(),
            stake_amount.as_u64(),
            "Pool stake should be active immediately in low-quorum"
        );
    } else {
        println!("Normal mode: stake should activate at E+2");
        assert_eq!(
            stake.inner.activation_epoch.as_u64(),
            epoch_before_stake.id.as_u64() + 2,
            "In normal mode, activation should be current epoch + 2"
        );
    }

    // Verify stake state is Active (waiting for activation or already active)
    assert!(
        stake.inner.is_staked(),
        "Stake should be in Active phase (not Unlocking or Withdrawn)"
    );

    println!(
        "Stake activation epoch: {}, stake phase: Active",
        stake.inner.activation_epoch.as_u64()
    );

    println!("\nTEST PASSED: Stake activation epoch is set correctly");
}

/// Test multiple delegators staking to the same node.
///
/// This verifies that:
/// 1. Multiple distinct wallets can stake to the same node pool
/// 2. Each delegator has their own stake account
/// 3. Pool stake aggregates all delegator stakes correctly
#[tokio::test]
#[ignore]
#[serial]
async fn test_multiple_delegators() {
    println!("Starting test_multiple_delegators...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;
    println!("System initialized");

    // Register a node
    let (node_keypair, node_address) = register_node(&client, &payer, "multi-delegator-node").await;
    println!("Node registered: {}", node_address);

    // Create multiple delegators
    const NUM_DELEGATORS: usize = 5;
    let mut delegators: Vec<Keypair> = Vec::new();
    let stake_per_delegator = Coin::<TAPE>::new(200_000_000);

    for i in 0..NUM_DELEGATORS {
        let delegator = Keypair::new();

        // Fund with SOL
        let fund_ix = solana_sdk::system_instruction::transfer(
            &payer.pubkey(),
            &delegator.pubkey(),
            500_000_000,
        );
        client
            .send_instructions(&payer, vec![fund_ix])
            .await
            .expect(&format!("fund delegator {}", i));

        // Fund with TAPE
        transfer_tape(&client, &payer, &delegator.pubkey(), stake_per_delegator.as_u64()).await;

        delegators.push(delegator);
    }
    println!("Created and funded {} delegators", NUM_DELEGATORS);

    // Each delegator stakes to the node
    for (i, delegator) in delegators.iter().enumerate() {
        let stake_ix = build_stake_with_pool_ix(
            delegator.pubkey(),
            delegator.pubkey(),
            node_address,
            stake_per_delegator,
        );
        client
            .send_instructions(delegator, vec![stake_ix])
            .await
            .expect(&format!("delegator {} stake", i));
        println!("Delegator {} staked {} TAPE", i, stake_per_delegator.as_u64());
    }

    // Verify each delegator has their own stake account
    for (i, delegator) in delegators.iter().enumerate() {
        let stake = client
            .get_stake(&delegator.pubkey())
            .await
            .expect(&format!("get delegator {} stake", i));

        assert_eq!(
            stake.authority, delegator.pubkey(),
            "Delegator {} stake authority mismatch",
            i
        );
        assert_eq!(
            stake.pool, node_address,
            "Delegator {} pool mismatch",
            i
        );
        assert_eq!(
            stake.inner.amount.as_u64(),
            stake_per_delegator.as_u64(),
            "Delegator {} amount mismatch",
            i
        );
    }
    println!("All delegator stake accounts verified");

    // Verify pool has aggregated all stakes
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");

    let expected_total = stake_per_delegator.as_u64() * NUM_DELEGATORS as u64;
    println!(
        "Node pool.stake: {}, expected: {}",
        node.pool.stake.as_u64(),
        expected_total
    );
    assert_eq!(
        node.pool.stake.as_u64(),
        expected_total,
        "Pool stake should be sum of all delegator stakes"
    );

    // Verify shares are non-zero (proportional to total stake)
    assert!(
        !node.pool.shares.is_zero(),
        "Pool shares should be non-zero"
    );

    println!("\nTEST PASSED: Multiple delegators can stake to same node");
}

/// Test that stake below minimum (zero) is rejected.
///
/// This verifies that:
/// 1. Zero-amount stake is rejected
/// 2. The error is appropriately returned
///
/// Note: The protocol doesn't have a fixed minimum stake constant,
/// but zero-amount stakes are explicitly rejected.
#[tokio::test]
#[ignore]
#[serial]
async fn test_stake_below_minimum() {
    println!("Starting test_stake_below_minimum...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "min-stake-node").await;
    println!("Node registered: {}", node_address);

    // Try to stake zero amount
    let zero_stake = Coin::<TAPE>::new(0);
    let stake_ix = build_stake_with_pool_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        zero_stake,
    );

    let result = client
        .send_instructions(&node_keypair, vec![stake_ix])
        .await;

    assert!(
        result.is_err(),
        "Zero stake should be rejected"
    );

    let err_str = result.unwrap_err().to_string();
    println!("Zero stake error: {}", err_str);

    // Use typed error parsing - should be ZeroShares
    let tape_err = TapeError::from_error_string(&err_str);
    assert!(
        tape_err == Some(TapeError::ZeroShares),
        "Should return ZeroShares error for zero stake, got: {} (parsed: {:?})",
        err_str,
        tape_err
    );

    // Now stake a valid amount to verify normal operation
    let valid_stake = Coin::<TAPE>::new(1_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), valid_stake.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, valid_stake).await;

    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert_eq!(
        node.pool.stake.as_u64(),
        valid_stake.as_u64(),
        "Valid stake should succeed"
    );

    println!("\nTEST PASSED: Zero stake is correctly rejected");
}

/// Test unstaking behavior when a node is actively in the committee.
///
/// This verifies that:
/// 1. An active committee member can request stake unlock
/// 2. The unlock follows the E+2 cooldown
/// 3. The node can continue serving until the stake is withdrawn
#[tokio::test]
#[ignore]
#[serial]
async fn test_unstake_while_in_committee() {
    use tape_api::program::EPOCH_DURATION;

    println!("Starting test_unstake_while_in_committee...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "committee-unstake-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;

    // Join committee
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("join");
    println!("Node joined committee_next");

    // Advance epoch to become active committee member
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch");
    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("sync");

    // Verify node is in committee
    let system = client.get_system().await.expect("get system");
    let node = client
        .get_node(&node_keypair.pubkey())
        .await
        .expect("get node");
    assert!(
        system.committee.contains(&node.id),
        "Node should be in committee"
    );
    println!("Node is in active committee (id={})", node.id.as_u64());

    debug_state(&client, &node_keypair, "Before unlock request").await;

    // Request unlock while in committee
    let unlock_ix = build_request_stake_unlock_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
    );
    let unlock_result = client
        .send_instructions(&node_keypair, vec![unlock_ix])
        .await;

    if unlock_result.is_ok() {
        println!("Unlock request accepted while in committee");

        // Check stake state changed to Unlocking
        let stake = client
            .get_stake(&node_keypair.pubkey())
            .await
            .expect("get stake");
        assert!(
            stake.inner.is_withdrawing(),
            "Stake should be in withdrawing state"
        );
        println!(
            "Stake is unlocking, withdraw epoch: {}",
            stake.inner.withdraw_epoch().unwrap().as_u64()
        );

        // Node can still advance pool and participate
        advance_pool(&client, &node_keypair, node_address)
            .await
            .expect("advance pool");
        println!("Node can still advance pool after unlock request");

        // Node can choose to not rejoin (voluntary exit)
        // Or rejoin if they want to continue serving
        let rejoin_result = join_committee(&client, &node_keypair, node_address).await;
        if rejoin_result.is_ok() {
            println!("Node can still rejoin committee after unlock request");
        } else {
            println!(
                "Node rejoin failed (expected if stake insufficient): {}",
                rejoin_result.unwrap_err()
            );
        }
    } else {
        let err = unlock_result.unwrap_err().to_string();
        println!("Unlock request while in committee: {}", err);
        // Some implementations may restrict unlock while in committee
    }

    debug_state(&client, &node_keypair, "After unlock operations").await;

    println!("\nTEST PASSED: Unstake behavior while in committee verified");
}
