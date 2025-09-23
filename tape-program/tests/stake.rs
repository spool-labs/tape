#![cfg(test)]

pub mod utils;
use utils::*;

use tape_api::prelude::*;

#[test]
fn test_stake() {
    let (mut svm, payer) = setup_environment();
    let treasury = initialize_program(&mut svm, &payer);

    let amount = TAPE::new(1000);

    let node_address = initialize_storage_node(&mut svm, &payer);
    let stake_address = stake_with_node(&mut svm, &payer, node_address, amount);

    let stake_ata = get_ata_address(&MINT_ADDRESS, &stake_address);
    let stake_balance = get_ata_balance(&svm, &stake_ata);
    let treasury_balance = get_ata_balance(&svm, &treasury);

    assert_eq!(stake_balance, amount.as_u64());
    assert_eq!(treasury_balance, MAX_SUPPLY - amount.as_u64());
}

