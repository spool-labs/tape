#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::{
    signer::Signer,
    signature::Keypair,
};
use tape_api::prelude::*;

#[test]
fn test_airdrop() {
    // Setup environment
    let (mut svm, payer) = setup_environment();

    // Initialize program
    initialize_program(&mut svm, &payer);

    // Create beneficiary and ATA
    let beneficiary = Keypair::new();
    let beneficiary_ata = create_ata(&mut svm, &payer, &MINT_ADDRESS, &beneficiary.pubkey());
    let expected_beneficiary_ata = get_ata_address(&MINT_ADDRESS, &beneficiary.pubkey());
    assert_eq!(beneficiary_ata, expected_beneficiary_ata,
        "Created ATA should match expected address");

    verify_treasury_account(&svm);
    verify_mint_account(&svm);
    verify_treasury_ata(&svm);

    // Test airdrop with standard amount
    let airdrop_amount = 500 * ONE_TAPE; // 500 TAPE
    airdrop(&mut svm, &payer, beneficiary_ata, TAPE::new(airdrop_amount));
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, airdrop_amount, 
        "ATA balance should match airdropped amount");

    // Test airdrop with large amount
    let large_amount = 1_000_000 * ONE_TAPE; // 1,000,000 TAPE
    airdrop(&mut svm, &payer, beneficiary_ata, TAPE::new(large_amount));
    let expected_balance = airdrop_amount + large_amount;
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, expected_balance, 
        "ATA balance should include large airdrop");

    // Test airdrop with zero amount
    airdrop(&mut svm, &payer, beneficiary_ata, TAPE::zero());
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, expected_balance, 
        "Zero-amount airdrop should not change balance");

    // Test airdrop to a new beneficiary
    let new_beneficiary = Keypair::new();
    let new_beneficiary_ata = create_ata(&mut svm, &payer, &MINT_ADDRESS, &new_beneficiary.pubkey());
    let expected_new_beneficiary_ata = get_ata_address(&MINT_ADDRESS, &new_beneficiary.pubkey());
    assert_eq!(new_beneficiary_ata, expected_new_beneficiary_ata, 
        "New beneficiary ATA should match expected address");

    let new_airdrop_amount = 1000 * ONE_TAPE; // 1000 TAPE
    airdrop(&mut svm, &payer, new_beneficiary_ata, TAPE::new(new_airdrop_amount));
    let new_ata_balance = get_ata_balance(&svm, &new_beneficiary_ata);
    assert_eq!(new_ata_balance, new_airdrop_amount, 
        "New beneficiary ATA balance should match airdropped amount");
}
