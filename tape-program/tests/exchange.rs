#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_stake() {
    let (mut svm, payer) = setup_environment();

    initialize_program(&mut svm, &payer);

    // Default exchange: authority = treasury PDA
    let (treasury, _) = treasury_pda();
    let (exchange, _) = exchange_pda(treasury);
    let (exchange_token, _) = exchange_ata(exchange);

    // Default exchange should have TAPE tokens
    let balance = get_ata_balance(&svm, &exchange_token);
    assert!(balance > 0);

    // Prepare payer token account for swaps and transfers
    create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());
    let token = get_ata_address(&MINT_ADDRESS, &payer.pubkey());

    // Swap SOL -> TAPE using default exchange
    let payer_tape = get_ata_balance(&svm, &token);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let exchange_sol = get_balance(&svm, &exchange);

    let sol_in = SOL::new(100);
    swap_for_tape(
        &mut svm,
        &payer,
        token,
        exchange,
        sol_in,
    );

    let payer_tape_after = get_ata_balance(&svm, &token);
    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);
    let exchange_sol_after = get_balance(&svm, &exchange);

    // With 1:1 rate, base units should match
    assert_eq!(
        payer_tape_after - payer_tape,
        sol_in.as_u64(),
        "Payer token account should receive TAPE from swap"
    );
    assert_eq!(
        exchange_tape - exchange_tape_after,
        sol_in.as_u64(),
        "Default exchange token account should send TAPE"
    );
    assert_eq!(
        exchange_sol_after - exchange_sol,
        sol_in.as_u64(),
        "Default exchange should receive SOL"
    );

    // Now test deposit and withdraw TAPE on a new, user-owned exchange
    initialize_exchange(&mut svm, &payer);
    let (exchange, _) = exchange_pda(payer.pubkey());
    let (exchange_token, _) = exchange_ata(exchange);

    // Deposit TAPE into user exchange
    let tape_amount = TAPE::new(100);
    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let payer_tape = get_ata_balance(&svm, &token);

    deposit_tape(
        &mut svm,
        &payer,
        token,
        exchange,
        tape_amount,
    );

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

    // Withdraw a portion of TAPE back to payer
    let tape_amount = TAPE::new(100);

    let exchange_tape = get_ata_balance(&svm, &exchange_token);
    let payer_tape = get_ata_balance(&svm, &token);

    withdraw_tape(
        &mut svm,
        &payer,
        token,
        exchange,
        tape_amount,
    );

    let exchange_tape_after = get_ata_balance(&svm, &exchange_token);
    let payer_tape_after = get_ata_balance(&svm, &token);

    assert_eq!(
        exchange_tape - exchange_tape_after,
        tape_amount.as_u64(),
        "Exchange token account should send withdrawn TAPE"
    );
    assert_eq!(
        payer_tape_after - payer_tape,
        tape_amount.as_u64(),
        "Payer token account should receive withdrawn TAPE"
    );

    // Deposit SOL into the user exchange, then withdraw SOL
    let sol_amount = SOL::new(200);
    let exchange_sol = get_balance(&svm, &exchange);

    deposit_sol(&mut svm, &payer, exchange, sol_amount);

    let exchange_sol_after = get_balance(&svm, &exchange);

    // Only assert on the exchange delta; the payer delta includes the transaction fee
    assert_eq!(
        exchange_sol_after - exchange_sol,
        sol_amount.as_u64(),
        "Exchange should receive deposited SOL"
    );

    // Withdraw part of the SOL back to payer
    let sol_amount = SOL::new(120);
    let exchange_sol = get_balance(&svm, &exchange);

    withdraw_sol(&mut svm, &payer, exchange, sol_amount);

    let exchange_sol_after = get_balance(&svm, &exchange);

    // Only assert on the exchange delta; payer net lamports change includes the fee
    assert_eq!(
        exchange_sol - exchange_sol_after,
        sol_amount.as_u64(),
        "Exchange should send withdrawn SOL"
    );

}
