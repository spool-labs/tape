#![cfg(feature = "airdrop")]
#![cfg(test)]

pub mod utils;
use utils::*;

use solana_sdk::{
    signer::Signer,
    transaction::Transaction,
    pubkey::Pubkey,
    signature::Keypair,
};
use litesvm::{types::TransactionResult, LiteSVM};
use tape_api::prelude::*;
use tape_api::instruction;

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
    assert_eq!(beneficiary_ata, expected_beneficiary_ata, "Created ATA should match expected address");

    // Verify treasury and mint accounts
    verify_treasury_account(&svm);
    verify_mint_account(&svm);
    verify_treasury_ata(&svm);

    // Test airdrop with standard amount
    let airdrop_amount = 500 * ONE_TAPE; // 500 TAPE
    perform_airdrop(&mut svm, &payer, beneficiary_ata, airdrop_amount);
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, airdrop_amount, "ATA balance should match airdropped amount");

    // Test airdrop with large amount
    let large_amount = 1_000_000 * ONE_TAPE; // 1,000,000 TAPE
    perform_airdrop(&mut svm, &payer, beneficiary_ata, large_amount);
    let expected_balance = airdrop_amount + large_amount;
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, expected_balance, "ATA balance should include large airdrop");

    // Test airdrop with zero amount
    perform_airdrop(&mut svm, &payer, beneficiary_ata, 0);
    let ata_balance = get_ata_balance(&svm, &beneficiary_ata);
    assert_eq!(ata_balance, expected_balance, "Zero-amount airdrop should not change balance");

    // Test airdrop to a new beneficiary
    let new_beneficiary = Keypair::new();
    let new_beneficiary_ata = create_ata(&mut svm, &payer, &MINT_ADDRESS, &new_beneficiary.pubkey());
    let expected_new_beneficiary_ata = get_ata_address(&MINT_ADDRESS, &new_beneficiary.pubkey());
    assert_eq!(new_beneficiary_ata, expected_new_beneficiary_ata, "New beneficiary ATA should match expected address");
    let new_airdrop_amount = 1000 * ONE_TAPE; // 1000 TAPE
    perform_airdrop(&mut svm, &payer, new_beneficiary_ata, new_airdrop_amount);
    let new_ata_balance = get_ata_balance(&svm, &new_beneficiary_ata);
    assert_eq!(new_ata_balance, new_airdrop_amount, "New beneficiary ATA balance should match airdropped amount");

    // Test airdrop to non-existent ATA (should fail)
    let third_beneficiary = Keypair::new();
    let third_beneficiary_ata = get_ata_address(&MINT_ADDRESS, &third_beneficiary.pubkey());
    let third_airdrop_amount = 750 * ONE_TAPE; // 750 TAPE
    let result = try_airdrop(&mut svm, &payer, third_beneficiary_ata, third_airdrop_amount);
    assert!(result.is_err(), "Airdrop to non-existent ATA should fail");
}

fn setup_environment() -> (LiteSVM, Keypair) {
    let mut svm = setup_svm();
    let payer = create_payer(&mut svm);
    (svm, payer)
}

fn initialize_program(svm: &mut LiteSVM, payer: &Keypair) {
    let payer_pk = payer.pubkey();
    let ix = instruction::program::build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok(), "Program initialization failed");
}

fn verify_treasury_account(svm: &LiteSVM) {
    let (treasury_address, _treasury_bump) = treasury_pda();
    let _treasury_account = svm
        .get_account(&treasury_address)
        .expect("Treasury account should exist");
}

fn verify_mint_account(svm: &LiteSVM) {
    let (mint_address, _mint_bump) = mint_pda();
    let mint = get_mint(svm, &mint_address);
    assert_eq!(mint.supply, MAX_SUPPLY, "Mint supply should be MAX_SUPPLY");
    assert_eq!(mint.decimals, TOKEN_DECIMALS, "Mint decimals should match TOKEN_DECIMALS");
}

fn verify_treasury_ata(svm: &LiteSVM) {
    let (treasury_ata_address, _ata_bump) = treasury_ata();
    let account = svm
        .get_account(&treasury_ata_address)
        .expect("Treasury ATA should exist");
    assert!(!account.data.is_empty(), "Treasury ATA data should not be empty");
}

fn perform_airdrop(
    svm: &mut LiteSVM,
    payer: &Keypair,
    beneficiary_ata: Pubkey,
    amount: u64,
) {
    let result = try_airdrop(svm, payer, beneficiary_ata, amount);
    assert!(result.is_ok(), "Airdrop transaction failed: {:?}", result.err());
}

fn try_airdrop(
    svm: &mut LiteSVM,
    payer: &Keypair,
    beneficiary_ata: Pubkey,
    amount: u64,
) -> TransactionResult {
    let payer_pk = payer.pubkey();
    let blockhash = svm.latest_blockhash();
    let ix = instruction::program::build_airdrop_ix(payer_pk, beneficiary_ata, amount);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    send_tx(svm, tx)
}
