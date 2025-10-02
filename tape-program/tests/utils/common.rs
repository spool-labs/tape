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

pub fn get_system_state(svm: &LiteSVM) -> System {
    let account = svm.get_account(&SYSTEM_ADDRESS).unwrap();
    *System::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_archive_state(svm: &LiteSVM, archive: &Pubkey) -> Archive {
    let account = svm.get_account(archive).unwrap();
    *Archive::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_epoch_state(svm: &LiteSVM, epoch: &Pubkey) -> Epoch {
    let account = svm.get_account(epoch).unwrap();
    *Epoch::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_treasury_state(svm: &LiteSVM, treasury: &Pubkey) -> Treasury {
    let account = svm.get_account(treasury).unwrap();
    *Treasury::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_exchange_state(svm: &LiteSVM, exchange: &Pubkey) -> Exchange {
    let account = svm.get_account(exchange).unwrap();
    *Exchange::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_council_state(svm: &LiteSVM, council: &Pubkey) -> Council {
    let account = svm.get_account(council).unwrap();
    *Council::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_storage_node_state(svm: &LiteSVM, node: &Pubkey) -> StorageNode {
    let account = svm.get_account(node).unwrap();
    *StorageNode::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_staked_tape_state(svm: &LiteSVM, stake: &Pubkey) -> StakedTape {
    let account = svm.get_account(stake).unwrap();
    *StakedTape::unpack_with_discriminator(&account.data).unwrap()
}

pub fn get_tape_state(svm: &LiteSVM, resource: &Pubkey) -> TapeResource {
    let account = svm.get_account(resource).unwrap();
    *TapeResource::unpack_with_discriminator(&account.data).unwrap()
}


pub fn initialize_program(
    svm: &mut LiteSVM,
    payer: &Keypair
) -> Pubkey {
    let payer_pk = payer.pubkey();
    let payer_ata = get_ata_address(&MINT_ADDRESS, &payer_pk);

    let ix = build_initialize_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
    payer_ata
}

pub fn initialize_exchange(
    svm: &mut LiteSVM,
    payer: &Keypair
) -> Pubkey {
    let payer_pk = payer.pubkey();
    let (exchange, _) = exchange_pda(payer_pk);

    let ix = build_register_exchange_ix(payer_pk);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
    exchange
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

    svm.expire_blockhash();
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

    svm.expire_blockhash();
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

    svm.expire_blockhash();
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

    svm.expire_blockhash();
    assert!(res.is_ok());
}

pub fn set_exchange_rate(
    svm: &mut LiteSVM,
    payer: &Keypair,
    exchange: Pubkey,
    tape: u64,
    sol: u64,
) {
    let payer_pk = payer.pubkey();
    let ix = build_set_exchange_rate_ix(payer_pk, exchange, tape, sol);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
}

pub fn swap_for_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    payer_ata: Pubkey,
    exchange: Pubkey,
    amount_sol: Coin<SOL>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_swap_for_tape_ix(payer_pk, payer_ata, exchange, amount_sol);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
}

pub fn swap_for_sol(
    svm: &mut LiteSVM,
    payer: &Keypair,
    payer_ata: Pubkey,
    exchange: Pubkey,
    amount_tape: Coin<TAPE>,
) {
    let payer_pk = payer.pubkey();
    let ix = build_swap_for_sol_ix(payer_pk, payer_ata, exchange, amount_tape);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
}

pub fn initialize_storage_node(
    svm: &mut LiteSVM,
    payer: &Keypair
) -> Pubkey {
    let payer_pk = payer.pubkey();
    let (node_address, _) = storage_node_pda(payer_pk);

    let name = to_name("Test Node");
    let commission_rate = BasisPoints::new(1000);
    let network_address = NetworkAddress::default();
    let network_tls = Pubkey::new_unique();

    let ix = build_register_node_ix(
        payer_pk, name, commission_rate, network_address, network_tls);

    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
    node_address
}

pub fn reserve_tape(
    svm: &mut LiteSVM,
    payer: &Keypair,
    storage_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Pubkey {
    let payer_pk = payer.pubkey();
    let (resource_address, _) = resource_pda(payer_pk);

    let ix = build_reserve_tape_ix(payer_pk, storage_units, start_epoch, end_epoch);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
    resource_address
}

pub fn stake_with_node(
    svm: &mut LiteSVM,
    payer: &Keypair,
    node_address: Pubkey,
    amount: Coin<TAPE>,
) -> Pubkey {
    let payer_pk = payer.pubkey();
    let (stake_address, _) = staked_tape_pda(payer_pk, node_address);

    let ix = build_stake_ix(payer_pk, node_address, amount);
    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[payer], blockhash);
    let res = send_tx(svm, tx);

    svm.expire_blockhash();
    assert!(res.is_ok());
    stake_address
}

