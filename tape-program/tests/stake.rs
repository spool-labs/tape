#![cfg(test)]

pub mod utils;
use utils::*;

use tape_api::prelude::*;

#[test]
fn test_stake() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    let amount = TAPE::new(1000);

    let node_address = initialize_storage_node(&mut svm, &payer);
    let stake_address = stake_with_node(&mut svm, &payer, node_address, amount);

    let stake_ata = get_ata_address(&MINT_ADDRESS, &stake_address);
    let ata_balance = get_ata_balance(&svm, &stake_ata);

    assert_eq!(ata_balance, amount.as_u64());
}

