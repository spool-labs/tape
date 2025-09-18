#![cfg(test)]

pub mod utils;
use steel::Pubkey;
use utils::*;

use solana_sdk::{
    signer::Signer,
    transaction::Transaction,
    signature::Keypair,
};
use litesvm::LiteSVM;
use tape_api::{
    instruction,
    utils::to_name,
    types::{BasisPoints, NetworkAddress}
};

#[test]
fn test_stake() {
    // Setup environment
    let (mut svm, payer) = setup_environment();

    // Initialize program
    initialize_program(&mut svm, &payer);

    // Initialize pool
    initialize_pool(&mut svm, &payer);
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

fn initialize_pool(svm: &mut LiteSVM, payer: &Keypair) {
    let payer_pk = payer.pubkey();

    let name = to_name("Test Pool");
    let commission_rate = BasisPoints::new(1000);
    let network_address = NetworkAddress::default();
    let network_tls = Pubkey::new_unique();

    let ix = instruction::pool::build_register_ix(
        payer_pk, name, commission_rate, network_address, network_tls);

    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}
