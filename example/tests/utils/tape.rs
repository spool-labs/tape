use solana_sdk::{
    signature::Keypair, 
    signer::Signer,
    transaction::Transaction,
    pubkey::Pubkey
};

use super::send_tx;
use litesvm::LiteSVM;

use tape_api::prelude::*;
use tape_api::instruction::program::{
    build_initialize_ix,
};
use tape_api::instruction::tape::{
    build_create_ix,
};

pub fn init_tape_program(svm: &mut LiteSVM, payer: &Keypair) {
    let payer_pk = payer.pubkey();

    let ix = build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn create_tape(svm: &mut LiteSVM, payer: &Keypair, tape_name: &str) -> (Pubkey, Pubkey) {
    let payer_pk = payer.pubkey();
    let (tape_address, _) = tape_pda(payer_pk, &to_name(tape_name));
    let (writer_address, _) = writer_pda(tape_address);

    let blockhash = svm.latest_blockhash();
    let ix = build_create_ix(payer_pk, tape_name);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(svm, tx);
    assert!(res.is_ok());

    (tape_address, writer_address)
}
