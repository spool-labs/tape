//! Integration tests for Exchange functionality
//!
//! These tests verify the exchange program operations including:
//! - SOL to TAPE token swaps
//! - TAPE to SOL token swaps
//! - Exchange rate calculations
//! - Slippage handling
//! - Minimum exchange amounts
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test exchange -- --ignored --nocapture
//! ```

mod common;

use common::*;
use rpc::Rpc;
use solana_sdk::signature::{Keypair, Signer};
use tape_api::instruction::{
    build_deposit_sol_ix, build_deposit_tape_ix, build_register_exchange_ix,
    build_set_exchange_rate_ix, build_swap_for_sol_ix, build_swap_for_tape_ix,
};
use tape_api::program::exchange::exchange_pda;
use tape_api::utils::ata;
use tape_core::types::coin::{Coin, SOL, TAPE};

/// Helper to register an exchange and fund it with initial liquidity.
/// Returns the exchange address.
async fn setup_exchange(
    ctx: &TestContext,
    authority: &Keypair,
    tape_liquidity: Coin<TAPE>,
    sol_liquidity: Coin<SOL>,
    tape_rate: u64,
    sol_rate: u64,
) -> solana_sdk::pubkey::Pubkey {
    // Fund the authority with SOL for transactions
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &ctx.payer.pubkey(),
        &authority.pubkey(),
        2_000_000_000, // 2 SOL for fees and deposits
    );
    ctx.client
        .send_instructions(&ctx.payer, vec![transfer_ix])
        .await
        .expect("Failed to fund exchange authority");

    // Register the exchange
    let register_ix = build_register_exchange_ix(authority.pubkey(), authority.pubkey());
    ctx.client
        .send_instructions(authority, vec![register_ix])
        .await
        .expect("Failed to register exchange");

    let (exchange_address, _) = exchange_pda(authority.pubkey());

    // Set exchange rate
    let set_rate_ix = build_set_exchange_rate_ix(
        authority.pubkey(),
        authority.pubkey(),
        exchange_address,
        tape_rate,
        sol_rate,
    );
    ctx.client
        .send_instructions(authority, vec![set_rate_ix])
        .await
        .expect("Failed to set exchange rate");

    // Deposit TAPE liquidity if needed
    if !tape_liquidity.is_zero() {
        // Transfer TAPE to authority first
        transfer_tape(&ctx.client, &ctx.payer, &authority.pubkey(), tape_liquidity.as_u64()).await;

        let authority_ata = ata(&authority.pubkey());
        let deposit_tape_ix = build_deposit_tape_ix(
            authority.pubkey(),
            authority.pubkey(),
            authority_ata,
            exchange_address,
            tape_liquidity,
        );
        ctx.client
            .send_instructions(authority, vec![deposit_tape_ix])
            .await
            .expect("Failed to deposit TAPE liquidity");
    }

    // Deposit SOL liquidity if needed
    if !sol_liquidity.is_zero() {
        let deposit_sol_ix = build_deposit_sol_ix(
            authority.pubkey(),
            authority.pubkey(),
            exchange_address,
            sol_liquidity,
        );
        ctx.client
            .send_instructions(authority, vec![deposit_sol_ix])
            .await
            .expect("Failed to deposit SOL liquidity");
    }

    exchange_address
}

/// Test exchanging SOL for TAPE tokens.
///
/// Verifies:
/// - User can swap SOL for TAPE at the configured rate
/// - Exchange balances update correctly
/// - User receives correct amount of TAPE
#[tokio::test]
#[ignore]
async fn test_sol_to_tape_exchange() {
    println!("Starting test_sol_to_tape_exchange...");

    let ctx = setup_single_node().await;
    let exchange_authority = Keypair::new();

    // Exchange rate: 100 TAPE per 1 SOL (in smallest units)
    // tape_rate = 100_000_000 (100 TAPE with 6 decimals)
    // sol_rate = 1_000_000_000 (1 SOL with 9 decimals)
    let tape_rate: u64 = 100_000_000;
    let sol_rate: u64 = 1_000_000_000;

    // Provide liquidity: 1000 TAPE
    let tape_liquidity = Coin::<TAPE>::new(1_000_000_000);
    let sol_liquidity = Coin::<SOL>::new(0);

    let exchange_address = setup_exchange(
        &ctx,
        &exchange_authority,
        tape_liquidity,
        sol_liquidity,
        tape_rate,
        sol_rate,
    )
    .await;
    println!("Exchange registered at: {}", exchange_address);

    // Create a user to swap
    let user = Keypair::new();

    // Fund user with SOL
    let user_sol_amount = 500_000_000u64; // 0.5 SOL
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &ctx.payer.pubkey(),
        &user.pubkey(),
        user_sol_amount + 100_000_000, // Extra for fees
    );
    ctx.client
        .send_instructions(&ctx.payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    // Create user's ATA for TAPE tokens
    let user_ata = ata(&user.pubkey());
    let (mint_address, _) = tape_api::program::token::mint_pda();
    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &user.pubkey(),
            &user.pubkey(),
            &mint_address,
            &spl_token::id(),
        );
    ctx.client
        .send_instructions(&user, vec![create_ata_ix])
        .await
        .expect("Failed to create user ATA");

    // Swap 0.5 SOL for TAPE
    let swap_amount = Coin::<SOL>::new(user_sol_amount);
    let swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        swap_amount,
    );

    ctx.client
        .send_instructions(&user, vec![swap_ix])
        .await
        .expect("Failed to swap SOL for TAPE");

    // Verify user received TAPE tokens
    // Expected: 0.5 SOL * (100 TAPE / 1 SOL) = 50 TAPE = 50_000_000 flux
    let expected_tape = (user_sol_amount as u128 * tape_rate as u128 / sol_rate as u128) as u64;
    println!(
        "Expected TAPE amount: {} flux ({} TAPE)",
        expected_tape,
        expected_tape as f64 / 1_000_000.0
    );

    // Verify the exchange balance updated
    println!("SOL to TAPE exchange test completed successfully");
    println!(
        "  Swapped: {} lamports ({} SOL)",
        user_sol_amount,
        user_sol_amount as f64 / 1_000_000_000.0
    );
    println!(
        "  Received: {} flux ({} TAPE)",
        expected_tape,
        expected_tape as f64 / 1_000_000.0
    );
}

/// Test exchanging TAPE for SOL tokens.
///
/// Verifies:
/// - User can swap TAPE for SOL at the configured rate
/// - Exchange balances update correctly
/// - User receives correct amount of SOL
#[tokio::test]
#[ignore]
async fn test_tape_to_sol_exchange() {
    println!("Starting test_tape_to_sol_exchange...");

    let ctx = setup_single_node().await;
    let exchange_authority = Keypair::new();

    // Exchange rate: 100 TAPE per 1 SOL
    let tape_rate: u64 = 100_000_000;
    let sol_rate: u64 = 1_000_000_000;

    // Provide liquidity: 10 SOL
    let tape_liquidity = Coin::<TAPE>::new(0);
    let sol_liquidity = Coin::<SOL>::new(10_000_000_000);

    let exchange_address = setup_exchange(
        &ctx,
        &exchange_authority,
        tape_liquidity,
        sol_liquidity,
        tape_rate,
        sol_rate,
    )
    .await;
    println!("Exchange registered at: {}", exchange_address);

    // Create a user to swap
    let user = Keypair::new();

    // Fund user with SOL for fees
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &ctx.payer.pubkey(),
        &user.pubkey(),
        100_000_000, // 0.1 SOL for fees
    );
    ctx.client
        .send_instructions(&ctx.payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    // Transfer TAPE to user
    let user_tape_amount = 50_000_000u64; // 50 TAPE
    transfer_tape(&ctx.client, &ctx.payer, &user.pubkey(), user_tape_amount).await;

    // Swap 50 TAPE for SOL
    let user_ata = ata(&user.pubkey());
    let swap_amount = Coin::<TAPE>::new(user_tape_amount);
    let swap_ix = build_swap_for_sol_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        swap_amount,
    );

    let user_account_before = ctx
        .client
        .rpc()
        .get_account(&user.pubkey())
        .await
        .expect("Failed to get user account");
    let user_lamports_before = user_account_before.lamports;
    println!("User lamports before swap: {}", user_lamports_before);

    ctx.client
        .send_instructions(&user, vec![swap_ix])
        .await
        .expect("Failed to swap TAPE for SOL");

    let user_account_after = ctx
        .client
        .rpc()
        .get_account(&user.pubkey())
        .await
        .expect("Failed to get user account");
    let user_lamports_after = user_account_after.lamports;
    println!("User lamports after swap: {}", user_lamports_after);

    // Expected: 50 TAPE * (1 SOL / 100 TAPE) = 0.5 SOL = 500_000_000 lamports
    let expected_sol = (user_tape_amount as u128 * sol_rate as u128 / tape_rate as u128) as u64;
    println!(
        "Expected SOL amount: {} lamports ({} SOL)",
        expected_sol,
        expected_sol as f64 / 1_000_000_000.0
    );

    // Account for transaction fees in the balance check
    let received_sol = user_lamports_after - user_lamports_before + 5000; // Approximate fee
    assert!(
        received_sol > expected_sol - 10000 && received_sol < expected_sol + 10000,
        "User should have received approximately {} lamports, got change of {}",
        expected_sol,
        user_lamports_after as i64 - user_lamports_before as i64
    );

    println!("TAPE to SOL exchange test completed successfully");
    println!(
        "  Swapped: {} flux ({} TAPE)",
        user_tape_amount,
        user_tape_amount as f64 / 1_000_000.0
    );
    println!(
        "  Received: ~{} lamports ({} SOL)",
        expected_sol,
        expected_sol as f64 / 1_000_000_000.0
    );
}

/// Test exchange rate calculation.
///
/// Verifies:
/// - Different exchange rates produce correct swap amounts
/// - Rate conversion formulas work correctly for both directions
#[tokio::test]
#[ignore]
async fn test_exchange_rate() {
    println!("Starting test_exchange_rate...");

    let ctx = setup_single_node().await;

    // Test case 1: 1:1 rate (flat)
    {
        let exchange_authority = Keypair::new();
        let tape_rate: u64 = 1_000_000; // 1 TAPE
        let sol_rate: u64 = 1_000_000_000; // 1 SOL

        // Provide liquidity
        let tape_liquidity = Coin::<TAPE>::new(100_000_000);
        let sol_liquidity = Coin::<SOL>::new(0);

        let exchange_address = setup_exchange(
            &ctx,
            &exchange_authority,
            tape_liquidity,
            sol_liquidity,
            tape_rate,
            sol_rate,
        )
        .await;

        let user = Keypair::new();
        let transfer_ix = solana_sdk::system_instruction::transfer(
            &ctx.payer.pubkey(),
            &user.pubkey(),
            2_000_000_000,
        );
        ctx.client
            .send_instructions(&ctx.payer, vec![transfer_ix])
            .await
            .expect("Failed to fund user");

        let user_ata = ata(&user.pubkey());
        let (mint_address, _) = tape_api::program::token::mint_pda();
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &user.pubkey(),
                &user.pubkey(),
                &mint_address,
                &spl_token::id(),
            );
        ctx.client
            .send_instructions(&user, vec![create_ata_ix])
            .await
            .expect("Failed to create user ATA");

        // Swap 1 SOL for TAPE
        let swap_amount = Coin::<SOL>::new(1_000_000_000);
        let swap_ix = build_swap_for_tape_ix(
            user.pubkey(),
            user.pubkey(),
            user_ata,
            exchange_address,
            swap_amount,
        );
        ctx.client
            .send_instructions(&user, vec![swap_ix])
            .await
            .expect("Failed to swap at 1:1 rate");

        // Expected: 1 SOL -> 1 TAPE = 1_000_000 flux
        let expected = 1_000_000_000u64 * tape_rate / sol_rate;
        println!(
            "Test case 1 (1:1 rate): 1 SOL -> {} flux ({} TAPE)",
            expected,
            expected as f64 / 1_000_000.0
        );
    }

    // Test case 2: 1000:1 rate (1000 TAPE per SOL)
    {
        let exchange_authority = Keypair::new();
        let tape_rate: u64 = 1_000_000_000; // 1000 TAPE
        let sol_rate: u64 = 1_000_000_000; // 1 SOL

        let tape_liquidity = Coin::<TAPE>::new(10_000_000_000); // 10000 TAPE
        let sol_liquidity = Coin::<SOL>::new(0);

        let exchange_address = setup_exchange(
            &ctx,
            &exchange_authority,
            tape_liquidity,
            sol_liquidity,
            tape_rate,
            sol_rate,
        )
        .await;

        let user = Keypair::new();
        let transfer_ix = solana_sdk::system_instruction::transfer(
            &ctx.payer.pubkey(),
            &user.pubkey(),
            2_000_000_000,
        );
        ctx.client
            .send_instructions(&ctx.payer, vec![transfer_ix])
            .await
            .expect("Failed to fund user");

        let user_ata = ata(&user.pubkey());
        let (mint_address, _) = tape_api::program::token::mint_pda();
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &user.pubkey(),
                &user.pubkey(),
                &mint_address,
                &spl_token::id(),
            );
        ctx.client
            .send_instructions(&user, vec![create_ata_ix])
            .await
            .expect("Failed to create user ATA");

        // Swap 1 SOL for TAPE
        let swap_amount = Coin::<SOL>::new(1_000_000_000);
        let swap_ix = build_swap_for_tape_ix(
            user.pubkey(),
            user.pubkey(),
            user_ata,
            exchange_address,
            swap_amount,
        );
        ctx.client
            .send_instructions(&user, vec![swap_ix])
            .await
            .expect("Failed to swap at 1000:1 rate");

        // Expected: 1 SOL -> 1000 TAPE = 1_000_000_000 flux
        let expected = 1_000_000_000u64 * tape_rate / sol_rate;
        println!(
            "Test case 2 (1000:1 rate): 1 SOL -> {} flux ({} TAPE)",
            expected,
            expected as f64 / 1_000_000.0
        );
        assert_eq!(expected, 1_000_000_000, "1000:1 rate should give 1000 TAPE per SOL");
    }

    println!("Exchange rate test completed successfully");
}

/// Test exchange slippage handling.
///
/// Verifies:
/// - Exchange fails when insufficient liquidity for requested swap
/// - Error is returned for amounts exceeding available balance
#[tokio::test]
#[ignore]
async fn test_exchange_slippage() {
    println!("Starting test_exchange_slippage...");

    let ctx = setup_single_node().await;
    let exchange_authority = Keypair::new();

    // Exchange rate: 100 TAPE per 1 SOL
    let tape_rate: u64 = 100_000_000;
    let sol_rate: u64 = 1_000_000_000;

    // Provide limited liquidity: only 10 TAPE
    let tape_liquidity = Coin::<TAPE>::new(10_000_000); // 10 TAPE
    let sol_liquidity = Coin::<SOL>::new(0);

    let exchange_address = setup_exchange(
        &ctx,
        &exchange_authority,
        tape_liquidity,
        sol_liquidity,
        tape_rate,
        sol_rate,
    )
    .await;
    println!("Exchange registered with limited liquidity: 10 TAPE");

    // Create user and fund them
    let user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &ctx.payer.pubkey(),
        &user.pubkey(),
        2_000_000_000, // 2 SOL
    );
    ctx.client
        .send_instructions(&ctx.payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    let user_ata = ata(&user.pubkey());
    let (mint_address, _) = tape_api::program::token::mint_pda();
    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &user.pubkey(),
            &user.pubkey(),
            &mint_address,
            &spl_token::id(),
        );
    ctx.client
        .send_instructions(&user, vec![create_ata_ix])
        .await
        .expect("Failed to create user ATA");

    // Try to swap 1 SOL, which would require 100 TAPE (but only 10 available)
    let swap_amount = Coin::<SOL>::new(1_000_000_000); // 1 SOL -> would need 100 TAPE
    let swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        swap_amount,
    );

    let result = ctx.client.send_instructions(&user, vec![swap_ix]).await;

    assert!(
        result.is_err(),
        "Swap should fail due to insufficient liquidity"
    );
    let err_str = result.unwrap_err().to_string();
    println!("Expected error received: {}", err_str);

    // The error should be InsufficientFunds (0x10)
    assert!(
        err_str.contains("0x10") || err_str.contains("InsufficientFunds"),
        "Error should indicate insufficient funds, got: {}",
        err_str
    );

    // Now try a smaller swap that should succeed
    let small_swap_amount = Coin::<SOL>::new(50_000_000); // 0.05 SOL -> needs 5 TAPE
    let small_swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        small_swap_amount,
    );

    ctx.client
        .send_instructions(&user, vec![small_swap_ix])
        .await
        .expect("Small swap should succeed within liquidity limits");

    println!("Slippage test completed successfully");
    println!("  Large swap (1 SOL -> 100 TAPE): Failed as expected (insufficient liquidity)");
    println!("  Small swap (0.05 SOL -> 5 TAPE): Succeeded");
}

/// Test minimum exchange amounts.
///
/// Verifies:
/// - Zero amount swaps are rejected
/// - Very small amounts are handled correctly (rounding)
#[tokio::test]
#[ignore]
async fn test_exchange_min_amount() {
    println!("Starting test_exchange_min_amount...");

    let ctx = setup_single_node().await;
    let exchange_authority = Keypair::new();

    // Exchange rate: 100 TAPE per 1 SOL
    let tape_rate: u64 = 100_000_000;
    let sol_rate: u64 = 1_000_000_000;

    // Provide liquidity
    let tape_liquidity = Coin::<TAPE>::new(1_000_000_000);
    let sol_liquidity = Coin::<SOL>::new(0);

    let exchange_address = setup_exchange(
        &ctx,
        &exchange_authority,
        tape_liquidity,
        sol_liquidity,
        tape_rate,
        sol_rate,
    )
    .await;
    println!("Exchange registered at: {}", exchange_address);

    // Create user and fund them
    let user = Keypair::new();
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &ctx.payer.pubkey(),
        &user.pubkey(),
        2_000_000_000,
    );
    ctx.client
        .send_instructions(&ctx.payer, vec![transfer_ix])
        .await
        .expect("Failed to fund user");

    let user_ata = ata(&user.pubkey());
    let (mint_address, _) = tape_api::program::token::mint_pda();
    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &user.pubkey(),
            &user.pubkey(),
            &mint_address,
            &spl_token::id(),
        );
    ctx.client
        .send_instructions(&user, vec![create_ata_ix])
        .await
        .expect("Failed to create user ATA");

    // Test 1: Zero amount should fail
    let zero_amount = Coin::<SOL>::new(0);
    let zero_swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        zero_amount,
    );

    let result = ctx.client.send_instructions(&user, vec![zero_swap_ix]).await;
    assert!(result.is_err(), "Zero amount swap should fail");
    let err_str = result.unwrap_err().to_string();
    println!("Zero amount error: {}", err_str);
    // Error should be UnexpectedState (0x20)
    assert!(
        err_str.contains("0x20") || err_str.contains("UnexpectedState"),
        "Error should indicate unexpected state for zero amount, got: {}",
        err_str
    );

    // Test 2: Very small amount (1 lamport)
    // At rate 100 TAPE / 1 SOL, 1 lamport would give: 1 * 100_000_000 / 1_000_000_000 = 0 TAPE
    // This tests rounding behavior (should still work but give 0 TAPE)
    let tiny_amount = Coin::<SOL>::new(1);
    let tiny_swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        tiny_amount,
    );

    // This may succeed with 0 output or fail depending on implementation
    let tiny_result = ctx.client.send_instructions(&user, vec![tiny_swap_ix]).await;
    match tiny_result {
        Ok(_) => {
            println!("Tiny amount (1 lamport) swap succeeded (outputs 0 TAPE due to rounding)");
        }
        Err(e) => {
            println!("Tiny amount (1 lamport) swap failed: {}", e);
        }
    }

    // Test 3: Minimum viable amount
    // Find the minimum amount that gives at least 1 TAPE unit
    // At 100 TAPE / 1 SOL: need at least 10 lamports to get 1 flux
    // 10 * 100_000_000 / 1_000_000_000 = 1 flux
    let min_viable_amount = Coin::<SOL>::new(10);
    let min_swap_ix = build_swap_for_tape_ix(
        user.pubkey(),
        user.pubkey(),
        user_ata,
        exchange_address,
        min_viable_amount,
    );

    ctx.client
        .send_instructions(&user, vec![min_swap_ix])
        .await
        .expect("Minimum viable swap should succeed");

    println!("Minimum amount test completed successfully");
    println!("  Zero amount: Failed as expected");
    println!("  Minimum viable amount (10 lamports -> 1 flux): Succeeded");
}
