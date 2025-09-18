#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_stake() {
    let (mut svm, payer) = setup_environment();

    initialize_program(&mut svm, &payer);
    initialize_pool(&mut svm, &payer);

    let amount = TAPE::new(1000);
    let ata = create_ata(&mut svm, &payer, &MINT_ADDRESS, &payer.pubkey());

    airdrop(&mut svm, &payer, ata, amount);

    let pre_balance = get_ata_balance(&svm, &TREASURY_ATA);

    {
        let (pool, _pool_bump) = pool_pda(payer.pubkey());
        stake_with_pool(&mut svm, &payer, ata, pool, TAPE::new(700));

        let ata_balance = get_ata_balance(&svm, &ata);
        assert_eq!(ata_balance, 300);
    }

    let post_balance = get_ata_balance(&svm, &TREASURY_ATA);
    assert_eq!(post_balance - pre_balance, 700);
}

