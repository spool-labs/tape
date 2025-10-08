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
use pretty_hex::*;
use bincode;

use spl_associated_token_account::get_associated_token_address;
use spl_token::state::{
    Account as Token,
    AccountState as TokenAccountStateEnum,
    Mint,
};

// Re-export checks
pub use mollusk_svm::result::Check;

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

pub fn pda(key: Pubkey, data: Vec<u8>) -> (Pubkey, Account) {
    (key, Account {
        lamports: Rent::default().minimum_balance(data.len()),
        data,
        owner: tape_api::ID,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn empty(key: Pubkey) -> (Pubkey, Account) {
    (key, Account::default())
}

pub fn ata_address(owner: &Pubkey) -> Pubkey {
    get_associated_token_address(owner, &MINT_ADDRESS)
}

pub fn ata(owner: Pubkey, amount: u64) -> (Pubkey, Account) {
    let address = ata_address(&owner);
    token(address, owner, amount)
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
        mint_authority: Some(TREASURY_ADDRESS).into(),
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

const METAPLEX_ELF : &[u8] = program_elf!("elfs/mpl_token_metadata.so");
pub fn mpl_program() -> (Pubkey, Account) {
    (
        mpl_token_metadata::ID, 
        mollusk_svm::program::create_program_account_loader_v2(METAPLEX_ELF)
    )
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
        checks: &[Check],
    ) {
        self.print_instruction(instruction);


        let result = self.mollusk.process_instruction(instruction, accounts);

        println!("size:\t{:?}", bincode::serialize(instruction).unwrap().len());
        println!("cu:\t{:?}", result.compute_units_consumed);

        println!("logs:\n");
        if let Some(logger) = &self.mollusk.logger {
            let guard = logger.borrow();
            for log in guard.get_recorded_content() {
                println!("\t{}", log);
            }
        }

        result.run_checks(checks);
    }

    fn print_instruction(&self, instruction: &Instruction) {
        println!("\n");
        println!("--------------------------------------------------------------------------------");
        println!("Program: {}", instruction.program_id);

        if !instruction.data.is_empty() {
            let discriminator = instruction.data[0];
            let ix_type = if let Ok(instruction_type) = TapeInstruction::try_from(discriminator) {
                format!("{:?}", instruction_type)
            } else {
                format!("Invalid (discriminator: {})", discriminator)
            };

            println!("\nix:\t{:?} ({})", ix_type, discriminator);
        }

        println!("accounts:");
        for (index, acc_meta) in instruction.accounts.iter().enumerate() {
            println!("\t{}: {:?}", index, acc_meta.pubkey);
        }

        println!("\ndata:\n\t{:?}", instruction.data);
        println!("\n\n{}\n", pretty_hex(&instruction.data));
    }
}

pub fn test_env(name: String) -> TestEnv {
    let name = format!("../target/deploy/{}", name);

    with_programs(&name, &[
        (&mpl_token_metadata::ID, METAPLEX_ELF),
    ])
}

pub fn with_programs(program_name: &str, programs: &[(&Pubkey, &'static [u8])]) -> TestEnv {
    let mut mollusk = Mollusk::new(&tape_api::ID, program_name);
    mollusk.logger = Some(solana_log_collector::LogCollector::new_ref());

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


