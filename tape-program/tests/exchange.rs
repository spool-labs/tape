#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_register_exchange() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());

    // Verify exchange account exists and has correct initial state
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();

    assert_eq!(exchange_data.authority, payer.pubkey());
    assert_eq!(exchange_data.balance_sol, SOL::zero());
    assert_eq!(exchange_data.balance_tape, TAPE::zero());
    assert_eq!(exchange_data.rate.tape, 1);
    assert_eq!(exchange_data.rate.sol, 1);
}

#[test]
fn test_set_exchange_rate() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());

    // Set exchange rate (e.g., 10 TAPE = 1 SOL)
    let tape_rate = 10;
    let sol_rate = 1;
    set_exchange_rate(&mut svm, &payer, exchange, tape_rate, sol_rate);

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();

    assert_eq!(exchange_data.rate.tape, tape_rate);
    assert_eq!(exchange_data.rate.sol, sol_rate);
}

#[test]
fn test_deposit_and_withdraw_tape() {
    let (mut svm, payer) = setup_environment();
    let treasury = initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_ata, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange
    let tape_amount = TAPE::new(100);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);

    deposit_tape(&mut svm, &payer, treasury, exchange, tape_amount);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);

    // Verify deposit balances
    assert_eq!(payer_balance - payer_balance_after, tape_amount.as_u64());
    assert_eq!(exchange_balance_after - exchange_balance, tape_amount.as_u64());

    // Verify exchange account data after deposit
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_tape.as_u64(), tape_amount.as_u64());

    // Withdraw a portion of TAPE
    let withdraw_amount = TAPE::new(50);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);

    withdraw_tape(&mut svm, &payer, treasury, exchange, withdraw_amount);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);

    // Verify withdrawal balances
    assert_eq!(payer_balance_after - payer_balance, withdraw_amount.as_u64());
    assert_eq!(exchange_balance - exchange_balance_after, withdraw_amount.as_u64());

    // Verify exchange account data after withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_tape.as_u64(), tape_amount.as_u64() - withdraw_amount.as_u64());

    // Withdraw full TAPE balance (amount = 0)
    let remaining_tape = tape_amount.as_u64() - withdraw_amount.as_u64();
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);

    withdraw_tape(&mut svm, &payer, treasury, exchange, TAPE::new(0));

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);

    // Verify full withdrawal balances
    assert_eq!(payer_balance_after - payer_balance, remaining_tape);
    assert_eq!(exchange_balance - exchange_balance_after, remaining_tape);
    assert_eq!(exchange_balance_after, 0);

    // Verify exchange account data after full withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_tape.as_u64(), 0);
}

#[test]
fn test_deposit_and_withdraw_sol() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());

    // Capture initial SOL balance
    let initial_balance = get_balance(&svm, &exchange);

    // Deposit SOL into user exchange
    let sol_amount = SOL::new(200);
    let exchange_balance = get_balance(&svm, &exchange);

    deposit_sol(&mut svm, &payer, exchange, sol_amount);

    let exchange_balance_after = get_balance(&svm, &exchange);

    // Verify SOL deposit
    assert_eq!(exchange_balance_after - exchange_balance, sol_amount.as_u64());

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), sol_amount.as_u64());

    // Withdraw SOL
    let withdraw_amount = SOL::new(120);
    let exchange_balance = get_balance(&svm, &exchange);

    withdraw_sol(&mut svm, &payer, exchange, withdraw_amount);

    let exchange_balance_after = get_balance(&svm, &exchange);

    // Verify SOL withdrawal
    assert_eq!(exchange_balance - exchange_balance_after, withdraw_amount.as_u64());

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), sol_amount.as_u64() - withdraw_amount.as_u64());

    // Withdraw full SOL balance (amount = 0)
    let remaining_sol = sol_amount.as_u64() - withdraw_amount.as_u64();
    let exchange_balance = get_balance(&svm, &exchange);

    withdraw_sol(&mut svm, &payer, exchange, SOL::new(0));

    let exchange_balance_after = get_balance(&svm, &exchange);

    // Verify full SOL withdrawal
    assert_eq!(exchange_balance - exchange_balance_after, remaining_sol);
    assert_eq!(exchange_balance_after, initial_balance);

    // Verify exchange account data after full withdrawal
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), 0);
}

#[test]
fn test_swap_for_tape() {
    let (mut svm, payer) = setup_environment();
    let treasury = initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_ata, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange for swapping
    let tape_amount = TAPE::new(1000);
    deposit_tape(&mut svm, &payer, treasury, exchange, tape_amount);

    // Get exchange rate
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    let rate = exchange_data.rate;

    // Perform SOL -> TAPE swap
    let sol_in = SOL::new(100);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol = get_balance(&svm, &exchange);

    swap_for_tape(&mut svm, &payer, treasury, exchange, sol_in);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // Calculate expected TAPE output
    let expected_tape = (sol_in.as_u64() * rate.tape) / rate.sol;

    // Verify balances
    assert_eq!(payer_balance_after - payer_balance, expected_tape);
    assert_eq!(exchange_balance - exchange_balance_after, expected_tape);
    assert_eq!(exchange_sol_after - exchange_sol, sol_in.as_u64());

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), sol_in.as_u64());
    assert_eq!(exchange_data.balance_tape.as_u64(), tape_amount.as_u64() - expected_tape);
}

#[test]
fn test_swap_for_sol() {
    let (mut svm, payer) = setup_environment();
    let treasury = initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_ata, _) = exchange_ata(exchange);

    // Set exchange rate (e.g., 2 TAPE = 1 SOL)
    let tape_rate = 2;
    let sol_rate = 1;
    set_exchange_rate(&mut svm, &payer, exchange, tape_rate, sol_rate);

    // Deposit SOL into user exchange for swapping
    let sol_amount = SOL::new(1000);
    deposit_sol(&mut svm, &payer, exchange, sol_amount);

    // Perform TAPE -> SOL swap
    let tape_in = TAPE::new(200);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol = get_balance(&svm, &exchange);

    swap_for_sol(&mut svm, &payer, treasury, exchange, tape_in);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // Calculate expected SOL output
    let expected_sol = (tape_in.as_u64() * sol_rate) / tape_rate;

    // Verify balances
    assert_eq!(exchange_balance_after - exchange_balance, tape_in.as_u64());
    assert_eq!(payer_balance - payer_balance_after, tape_in.as_u64());
    assert_eq!(exchange_sol - exchange_sol_after, expected_sol);

    // Verify exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), sol_amount.as_u64() - expected_sol);
    assert_eq!(exchange_data.balance_tape.as_u64(), tape_in.as_u64());
}

#[test]
fn test_swap_for_tape_with_rate_change() {
    let (mut svm, payer) = setup_environment();
    let treasury = initialize_program(&mut svm, &payer);

    // Initialize user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_ata, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange for swapping
    let tape_amount = TAPE::new(1_00_000);
    deposit_tape(&mut svm, &payer, treasury, exchange, tape_amount);

    // Set initial exchange rate (e.g., 100 TAPE = 1 SOL)
    let initial_tape_rate = 100;
    let initial_sol_rate = 1;
    set_exchange_rate(&mut svm, &payer, exchange, initial_tape_rate, initial_sol_rate);

    // Perform first SOL -> TAPE swap
    let sol_in = SOL::new(100);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol = get_balance(&svm, &exchange);

    swap_for_tape(&mut svm, &payer, treasury, exchange, sol_in);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // Calculate expected TAPE output for first swap
    let expected_tape = (sol_in.as_u64() * initial_tape_rate) / initial_sol_rate;

    // Verify balances for first swap
    assert_eq!(payer_balance_after - payer_balance, expected_tape);
    assert_eq!(exchange_balance - exchange_balance_after, expected_tape);
    assert_eq!(exchange_sol_after - exchange_sol, sol_in.as_u64());

    // Change exchange rate (e.g., 4 TAPE = 1 SOL)
    let new_tape_rate = 4;
    let new_sol_rate = 1;
    set_exchange_rate(&mut svm, &payer, exchange, new_tape_rate, new_sol_rate);

    // Verify new exchange rate
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.rate.tape, new_tape_rate);
    assert_eq!(exchange_data.rate.sol, new_sol_rate);

    // Perform second SOL -> TAPE swap
    let sol_in = SOL::new(100);
    let payer_balance = get_ata_balance(&svm, &treasury);
    let exchange_balance = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol = get_balance(&svm, &exchange);

    swap_for_tape(&mut svm, &payer, treasury, exchange, sol_in);

    let payer_balance_after = get_ata_balance(&svm, &treasury);
    let exchange_balance_after = get_ata_balance(&svm, &exchange_ata);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // Calculate expected TAPE output for second swap
    let expected_tape_2 = (sol_in.as_u64() * new_tape_rate) / new_sol_rate;

    // Verify balances for second swap
    assert_eq!(payer_balance_after - payer_balance, expected_tape_2);
    assert_eq!(exchange_balance - exchange_balance_after, expected_tape_2);
    assert_eq!(exchange_sol_after - exchange_sol, sol_in.as_u64());

    // Verify final exchange account data
    let account = svm.get_account(&exchange).unwrap();
    let exchange_data = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange_data.balance_sol.as_u64(), sol_in.as_u64() + sol_in.as_u64());
    assert_eq!(exchange_data.balance_tape.as_u64(), tape_amount.as_u64() - expected_tape - expected_tape_2);
}
