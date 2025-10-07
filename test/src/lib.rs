use tape_api::prelude::*;
use mollusk_svm::{
    program::{keyed_account_for_system_program, loader_keys::LOADER_V2},
    sysvar::Sysvars,
    Mollusk,
};

use mollusk_svm_programs_token::{
    associated_token as spl_ata_program,
    token as spl_token_program,
};

use solana_sdk::{
    account::Account,
    instruction::Instruction,
    program_pack::Pack,
    pubkey::Pubkey,
    rent::Rent,
};

use solana_program::program_option::COption;

use spl_token::state::{
    Account as Token,
    AccountState as TokenAccountStateEnum,
    Mint,
};

// Helpers for constructing test accounts.

pub fn sol(key: Pubkey, lamports: u64) -> (Pubkey, Account) {
    (key, Account {
        lamports,
        data: vec![],
        owner: solana_program::system_program::ID,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn empty(key: Pubkey) -> (Pubkey, Account) {
    (key, Account::default())
}

pub fn token(address: Pubkey, owner: Pubkey, amount: u64) -> (Pubkey, Account) {
    let state = Token {
        mint: MINT_ADDRESS,
        owner,
        amount,
        delegate: COption::None,
        state: TokenAccountStateEnum::Initialized,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };

    let mut data = vec![0u8; Token::LEN];
    Token::pack(state, &mut data).unwrap();

    (address, Account {
        lamports: Rent::default().minimum_balance(Token::LEN),
        data,
        owner: spl_token_program::ID,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn mint(supply: u64) -> (Pubkey, Account) {
    let mint_data = Mint {
        mint_authority: Some(MINT_ADDRESS).into(),
        supply,
        decimals: TOKEN_DECIMALS,
        is_initialized: true,
        freeze_authority: None.into(),
    };

    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_data, &mut data).unwrap();

    (MINT_ADDRESS, Account {
        lamports: Rent::default().minimum_balance(Mint::LEN),
        data,
        owner: spl_token_program::ID,
        executable: false,
        rent_epoch: 0,
    })
}

// Program and sysvar keyed accounts.

pub fn system_program() -> (Pubkey, Account) {
    keyed_account_for_system_program()
}

pub fn token_program() -> (Pubkey, Account) {
    spl_token_program::keyed_account()
}

pub fn ata_program() -> (Pubkey, Account) {
    spl_ata_program::keyed_account()
}

pub fn rent_sysvar() -> (Pubkey, Account) {
    Sysvars::default().keyed_account_for_rent_sysvar()
}

// Test environment

pub struct TestEnv {
    mollusk: Mollusk,
}

impl TestEnv {
    pub fn process_instruction(
        &self,
        instruction: &Instruction,
        accounts: &[(Pubkey, Account)],
    ) {
        self.mollusk.process_instruction(instruction, accounts);
    }
}

pub fn test_env(name: String) -> TestEnv {
    let name = format!("../target/deploy/{}", name);

    with_programs(&name, &[
        (&mpl_token_metadata::ID, program_elf!("elfs/mpl_token_metadata.so")),
    ])
}

pub fn with_programs(program_name: &str, programs: &[(&Pubkey, &'static [u8])]) -> TestEnv {
    let mut mollusk = Mollusk::new(&tape_api::ID, program_name);

    spl_token_program::add_program(&mut mollusk);
    spl_ata_program::add_program(&mut mollusk);

    for (id, elf) in programs {
        mollusk.add_program_with_elf_and_loader(id, elf, &LOADER_V2);
    }

    TestEnv { mollusk }
}

// ELF helpers

#[macro_export]
macro_rules! program_elf {
    ($relative_path:literal) => {
        include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $relative_path))
    };
}


