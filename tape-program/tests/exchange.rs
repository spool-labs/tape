#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_stake() {
    let (mut svm, payer) = setup_environment();

    initialize_program(&mut svm, &payer);
    initialize_exchange(&mut svm, &payer);

    let (exchange_address, _) = exchange_pda(payer.pubkey());
    let (exchange_ata, _) = exchange_ata(exchange_address);

    let pre_tape_balance = get_ata_balance(&svm, &exchange_ata);
    assert_eq!(pre_tape_balance, 0);

    let pre_sol_balance = get_balance(&svm, &exchange_address);
    assert!(pre_sol_balance > 0); // <- includes account rent

    let amout = SOL::new(10_000);
    deposit_sol(&mut svm, &payer, exchange_address, amout);

    let post_sol_balance = get_balance(&svm, &exchange_address);
    assert_eq!(post_sol_balance - pre_sol_balance, amout.as_u64());

    let account = svm.get_account(&exchange_address).unwrap();
    let exchange = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange.balance_sol, amout);

    withdraw_sol(&mut svm, &payer, exchange_address, amout);

    let account = svm.get_account(&exchange_address).unwrap();
    let exchange = Exchange::unpack_with_discriminator(&account.data).unwrap();
    assert_eq!(exchange.balance_sol, SOL::zero());

    let post_sol_balance = get_balance(&svm, &exchange_address);
    assert_eq!(post_sol_balance, pre_sol_balance);
}

