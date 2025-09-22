#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_create_exchange() {
    let (mut svm, payer) = setup_environment();

    initialize_program(&mut svm, &payer);
    initialize_exchange(&mut svm, &payer);
}

#[test]
fn test_default_exchange_swap() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Default exchange: authority = treasury PDA
    let (treasury, _) = treasury_pda();
    let (exchange, _) = exchange_pda(treasury);
    let (exchange_token, _) = exchange_ata(exchange);

    // Default exchange should have TAPE tokens
    let balance = get_ata_balance(&svm, &exchange_token);
    assert!(balance > 0);

    // Get exchange rate from account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    let rate = exchange_data.rate;

    // Prepare payer token account for swaps
    create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());
    let token = get_ata_address(&MINT_ADDRESS, &payer.pubkey());

    // Swap SOL -> TAPE using default exchange
    let payer_tape = get_ata_balance(&svm, &token);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let exchange_sol = get_balance(&svm, &exchange);

    let sol_in = SOL::new(100);
    swap_for_tape(&mut svm, &payer, token, exchange, sol_in);

    let payer_tape_after = get_ata_balance(&svm, &token);
    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // Calculate expected TAPE output: (sol_in * rate.tape) / rate.sol
    let expected_tape = (sol_in.as_u64() * rate.tape) / rate.sol;

    assert_eq!(
        payer_tape_after - payer_tape,
        expected_tape,
        "Payer token account should receive TAPE based on exchange rate"
    );
    assert_eq!(
        exchange_tape - exchange_tape_after,
        expected_tape,
        "Default exchange token account should send TAPE based on exchange rate"
    );
    assert_eq!(
        exchange_sol_after - exchange_sol,
        sol_in.as_u64(),
        "Default exchange should receive SOL"
    );

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_sol.as_u64(),
        exchange_sol_after - exchange_sol,
        "Exchange SOL balance should match the increase in account lamports"
    );
    assert_eq!(
        exchange_data.balance_tape.as_u64(),
        exchange_tape_after,
        "Exchange TAPE balance should match account data"
    );
}

#[test]
fn test_user_exchange_tape_operations() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Default exchange: authority = treasury PDA
    let (treasury, _) = treasury_pda();
    let (default_exchange, _) = exchange_pda(treasury);

    // Get default exchange rate
    let account = svm.get_account(&default_exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    let rate = exchange_data.rate;

    // Prepare payer token account
    create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());
    let token = get_ata_address(&MINT_ADDRESS, &payer.pubkey());

    // Swap SOL -> TAPE to give payer some TAPE tokens
    let sol_in = SOL::new(200);
    swap_for_tape(&mut svm, &payer, token, default_exchange, sol_in);

    // Verify payer has TAPE tokens
    let expected_tape = (sol_in.as_u64() * rate.tape) / rate.sol;
    let payer_tape = get_ata_balance(&svm, &token);
    assert_eq!(
        payer_tape, expected_tape,
        "Payer should have TAPE tokens after swap"
    );

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_token, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange
    let tape_amount = TAPE::new(100);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let payer_tape = get_ata_balance(&svm, &token);

    deposit_tape(&mut svm, &payer, token, exchange, tape_amount);

    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);
    let payer_tape_after = get_ata_balance(&svm, &token);

    assert_eq!(
        exchange_tape_after - exchange_tape,
        tape_amount.as_u64(),
        "Exchange token account should receive deposited TAPE"
    );
    assert_eq!(
        payer_tape - payer_tape_after,
        tape_amount.as_u64(),
        "Payer token account should send deposited TAPE"
    );

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_tape.as_u64(),
        tape_amount.as_u64(),
        "Exchange TAPE balance should match deposited amount"
    );

    // Withdraw a portion of TAPE back to payer
    let withdraw_amount = TAPE::new(50);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let payer_tape = get_ata_balance(&svm, &token);

    withdraw_tape(&mut svm, &payer, token, exchange, withdraw_amount);

    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);
    let payer_tape_after = get_ata_balance(&svm, &token);

    assert_eq!(
        exchange_tape - exchange_tape_after,
        withdraw_amount.as_u64(),
        "Exchange token account should send withdrawn TAPE"
    );
    assert_eq!(
        payer_tape_after - payer_tape,
        withdraw_amount.as_u64(),
        "Payer token account should receive withdrawn TAPE"
    );

    // Verify exchange account data after withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_tape.as_u64(),
        tape_amount.as_u64() - withdraw_amount.as_u64(),
        "Exchange TAPE balance should reflect withdrawal"
    );
}

#[test]
fn test_user_exchange_sol_operations() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());

    // Deposit SOL into user exchange
    let sol_amount = SOL::new(200);
    let exchange_sol = get_balance(&svm, &exchange);

    deposit_sol(&mut svm, &payer, exchange, sol_amount);

    let exchange_sol_after = get_balance(&svm, &exchange);

    assert_eq!(
        exchange_sol_after - exchange_sol,
        sol_amount.as_u64(),
        "Exchange should receive deposited SOL"
    );

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_sol.as_u64(),
        exchange_sol_after - exchange_sol,
        "Exchange SOL balance should match deposited amount"
    );

    // Withdraw part of the SOL back to payer
    let withdraw_amount = SOL::new(120);
    let exchange_sol = get_balance(&svm, &exchange);

    withdraw_sol(&mut svm, &payer, exchange, withdraw_amount);

    let exchange_sol_after = get_balance(&svm, &exchange);

    assert_eq!(
        exchange_sol - exchange_sol_after,
        withdraw_amount.as_u64(),
        "Exchange should send withdrawn SOL"
    );

    // Verify exchange account data after withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_sol.as_u64(),
        sol_amount.as_u64() - withdraw_amount.as_u64(),
        "Exchange SOL balance should reflect withdrawal"
    );
}

#[test]
fn test_withdraw_full_tape_balance() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Default exchange: authority = treasury PDA
    let (treasury, _) = treasury_pda();
    let (default_exchange, _) = exchange_pda(treasury);

    // Get default exchange rate
    let account = svm.get_account(&default_exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    let rate = exchange_data.rate;

    // Prepare payer token account
    create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());
    let token = get_ata_address(&MINT_ADDRESS, &payer.pubkey());

    // Swap SOL -> TAPE to give payer some TAPE tokens
    let sol_in = SOL::new(200);
    swap_for_tape(&mut svm, &payer, token, default_exchange, sol_in);

    // Verify payer has TAPE tokens
    let expected_tape = (sol_in.as_u64() * rate.tape) / rate.sol;
    let payer_tape = get_ata_balance(&svm, &token);
    assert_eq!(
        payer_tape, expected_tape,
        "Payer should have TAPE tokens after swap"
    );

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_token, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange
    let tape_amount = TAPE::new(100);
    deposit_tape(&mut svm, &payer, token, exchange, tape_amount);

    // Verify deposit
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    assert_eq!(
        exchange_tape, tape_amount.as_u64(),
        "Exchange token account should have deposited TAPE"
    );

    // Verify exchange account data before withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_tape.as_u64(),
        tape_amount.as_u64(),
        "Exchange TAPE balance should match deposited amount before withdrawal"
    );

    // Withdraw with amount = 0 (should withdraw full available balance)
    let payer_tape = get_ata_balance(&svm, &token);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);

    withdraw_tape(&mut svm, &payer, token, exchange, TAPE::new(0));

    let payer_tape_after = get_ata_balance(&svm, &token);
    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);

    // Check token account balances
    assert_eq!(
        exchange_tape - exchange_tape_after,
        tape_amount.as_u64(),
        "Exchange token account should send all available TAPE"
    );
    assert_eq!(
        payer_tape_after - payer_tape,
        tape_amount.as_u64(),
        "Payer token account should receive all available TAPE"
    );
    assert_eq!(
        exchange_tape_after, 0,
        "Exchange token account should be empty after full withdrawal"
    );

    // Verify exchange account data after withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(
        exchange_data.balance_tape.as_u64(),
        0,
        "Exchange TAPE balance should be zero after full withdrawal"
    );
}
