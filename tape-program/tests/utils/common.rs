use super::*;

use solana_sdk::{
    pubkey::Pubkey,
    signer::Signer,
    transaction::Transaction,
    signature::Keypair,
};
use litesvm::LiteSVM;

use tape_api::prelude::*;

pub fn setup_environment() -> (LiteSVM, Keypair) {
    let mut svm = setup_svm();
    let payer = create_payer(&mut svm);
    (svm, payer)
}

pub fn verify_treasury_account(svm: &LiteSVM) {
    let (treasury_address, _treasury_bump) = treasury_pda();
    let _treasury_account = svm
        .get_account(&treasury_address)
        .expect("Treasury account should exist");
}

pub fn verify_mint_account(svm: &LiteSVM) {
    let (mint_address, _mint_bump) = mint_pda();
    let mint = get_mint(svm, &mint_address);
    assert_eq!(mint.supply, MAX_SUPPLY, "Mint supply should be MAX_SUPPLY");
    assert_eq!(mint.decimals, TOKEN_DECIMALS, "Mint decimals should match TOKEN_DECIMALS");
}

pub fn verify_treasury_ata(svm: &LiteSVM) {
    let (treasury_ata_address, _ata_bump) = treasury_ata();
    let account = svm
        .get_account(&treasury_ata_address)
        .expect("Treasury ATA should exist");
    assert!(!account.data.is_empty(), "Treasury ATA data should not be empty");
}

pub fn airdrop(
    svm: &mut LiteSVM,
    payer: &Keypair,
    beneficiary_ata: Pubkey,
    amount: Coin<TAPE>,
) {
    let payer_pk = payer.pubkey();
    let blockhash = svm.latest_blockhash();
    let ix = build_airdrop_ix(payer_pk, beneficiary_ata, amount);
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn initialize_program(
    svm: &mut LiteSVM,
    payer: &Keypair
) {
    let payer_pk = payer.pubkey();
    let ix = build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn initialize_exchange(
    svm: &mut LiteSVM,
    payer: &Keypair
) {
    let payer_pk = payer.pubkey();
    let ix = build_register_exchange_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn deposit_sol(
    svm: &mut LiteSVM,
    payer: &Keypair,
    exchange: Pubkey,
    amount: Coin<SOL>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_deposit_sol_ix(payer_pk, exchange, amount);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn withdraw_sol(
    svm: &mut LiteSVM,
    payer: &Keypair,
    exchange: Pubkey,
    amount: Coin<SOL>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_withdraw_sol_ix(payer_pk, exchange, amount);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn deposit_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    payer_ata: Pubkey,
    exchange: Pubkey,
    amount: Coin<TAPE>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_deposit_tape_ix(payer_pk, payer_ata, exchange, amount);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn withdraw_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    payer_ata: Pubkey,
    exchange: Pubkey,
    amount: Coin<TAPE>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_withdraw_tape_ix(payer_pk, payer_ata, exchange, amount);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn initialize_storage_node(
    svm: &mut LiteSVM,
    payer: &Keypair
) {
    let payer_pk = payer.pubkey();

    let name = to_name("Test Node");
    let commission_rate = BasisPoints::new(1000);
    let network_address = NetworkAddress::default();
    let network_tls = Pubkey::new_unique();

    let ix = build_register_node_ix(
        payer_pk, name, commission_rate, network_address, network_tls);

    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

pub fn stake_with_pool(
    svm: &mut LiteSVM,
    payer: &Keypair,
    ata: Pubkey,
    pool: Pubkey,
    amount: Coin<TAPE>,
) {
    let payer_pk = payer.pubkey();

    let ix = build_stake_ix(
        payer_pk, ata, pool, amount
    );

    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    assert!(res.is_ok());
}

