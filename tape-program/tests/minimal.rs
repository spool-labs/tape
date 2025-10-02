#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::signer::Signer;
use tape_api::prelude::*;

#[test]
fn test_minimal() {
    let (mut svm, payer) = setup_environment();
    initialize_program(&mut svm, &payer);

    let treasury = get_ata_address(&MINT_ADDRESS, &payer.pubkey());
    let balance = get_ata_balance(&svm, &treasury);

    //let (council, _) = council_pda(EpochNumber::zero());
    //let council_state = get_council_state(&svm, &council);

    assert_eq!(balance, MAX_SUPPLY);
}

