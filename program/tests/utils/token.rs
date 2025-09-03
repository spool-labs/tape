use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use litesvm::{types::FailedTransactionMetadata, LiteSVM};
use litesvm_token::{
    CreateAssociatedTokenAccount, 
    CreateMint, 
    MintTo, 
    spl_token::state::{Account, Mint}, 
    get_spl_account,
};
use spl_associated_token_account::get_associated_token_address;

use crate::utils::SvmWithCUTracker;


pub fn create_mint(svm: &mut SvmWithCUTracker, payer_kp: &Keypair, owner_pk: &Pubkey, decimals: u8) -> Pubkey {
    let SvmWithCUTracker { svm, cu_tracker:_, payer:_ } = svm;
    CreateMint::new(svm, payer_kp)
        .authority(owner_pk)
        .decimals(decimals)
        .send()
        .unwrap()
}

pub fn create_ata(svm: &mut SvmWithCUTracker, mint_pk: &Pubkey, owner_pk: &Pubkey) -> Pubkey {
    let SvmWithCUTracker { svm, cu_tracker:_, payer } = svm;
    CreateAssociatedTokenAccount::new(svm, payer, mint_pk)
        .owner(owner_pk)
        .send()
        .unwrap()
}

pub fn get_ata_address(mint_pk: &Pubkey, owner_pk: &Pubkey) -> Pubkey {
    get_associated_token_address(owner_pk, mint_pk)
}

pub fn get_ata_balance(svm: &SvmWithCUTracker, ata: &Pubkey) -> u64 {
    let info : Account = get_spl_account(&svm.svm, ata).unwrap();
    info.amount
}

pub fn get_mint(svm: &LiteSVM, mint: &Pubkey) -> Mint {
    get_spl_account(svm, mint).unwrap()
}

pub fn mint_to(svm: &mut LiteSVM,
        payer: &Keypair,
        mint: &Pubkey,
        mint_owner: &Keypair,
        destination: &Pubkey,
        amount: u64,
) -> Result<(), FailedTransactionMetadata> {
    MintTo::new(svm, payer, mint, destination, amount)
        .owner(mint_owner)
        .send()
}
