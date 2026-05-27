use tape_api::program::prelude::*;
use tape_api::program;
use mollusk_svm::{
    program::{keyed_account_for_system_program, loader_keys::LOADER_V2},
    result::Config as CheckConfig,
    sysvar::Sysvars,
    Mollusk,
};

use mollusk_svm_programs_token::{
    associated_token as spl_ata_program,
    token as spl_token_program,
};

#[allow(deprecated)]
use solana_sdk::bpf_loader_upgradeable;
use solana_sdk::{
    account::Account,
    instruction::Instruction,
    program_pack::Pack,
    pubkey::Pubkey,
    rent::Rent,
};

pub(crate) const DEFAULT_LOADER_KEY: Pubkey = bpf_loader_upgradeable::id();

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

/// Seconds per epoch used by tests. Stored in `Epoch.preferences.epoch_duration`
/// (via test fixtures) and used to drive the commit gate at
/// `solana/programs/tapedrive/src/epoch/commit.rs:50`.
pub const TEST_EPOCH_DURATION: EpochDuration = EpochDuration(20);

/// Lower bound on aggregated epoch_duration written to System at StartNetwork
/// in test flows. Production picks its own (e.g. 2 days).
pub const TEST_MIN_EPOCH_DURATION: EpochDuration = EpochDuration(10);

/// Upper bound on aggregated epoch_duration written to System at StartNetwork
/// in test flows. Production picks its own (e.g. 14 days).
pub const TEST_MAX_EPOCH_DURATION: EpochDuration = EpochDuration(200);

// Helpers for constructing test accounts.

pub fn sol(key: impl Into<Pubkey>, lamports: u64) -> (Pubkey, Account) {
    let key = key.into();

    (key, Account {
        lamports,
        data: vec![],
        owner: solana_program::system_program::ID,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn pda(key: impl Into<Pubkey>, data: Vec<u8>, program: impl Into<Pubkey>) -> (Pubkey, Account) {
    let key = key.into();
    let program = program.into();

    (key, Account {
        lamports: rent(data.len()),
        data,
        owner: program,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn rent(space: usize) -> u64 {
    Rent::default().minimum_balance(space)
}

pub fn rent_token() -> u64 {
    rent(Token::LEN)
}

pub fn rent_mint() -> u64 {
    rent(Mint::LEN)
}

pub fn empty(key: impl Into<Pubkey>) -> (Pubkey, Account) {
    let key = key.into();

    (key, Account::default())
}

pub fn ata_address(owner: &Pubkey) -> Pubkey {
    get_associated_token_address(owner, &MINT_ADDRESS.into())
}

pub fn ata(owner: impl Into<Pubkey>, amount: u64) -> (Pubkey, Account) {
    let owner = owner.into();
    let address = ata_address(&owner);
    token(address, owner, amount)
}

pub fn token(address: impl Into<Pubkey>, owner: impl Into<Pubkey>, amount: u64) -> (Pubkey, Account) {
    let address = address.into();
    let owner = owner.into();

    let state = Token {
        mint: MINT_ADDRESS.into(),
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
        lamports: rent_token(),
        data,
        owner: spl_token_program::ID,
        executable: false,
        rent_epoch: 0,
    })
}

pub fn mint(supply: u64) -> (Pubkey, Account) {
    let mint_data = Mint {
        mint_authority: Some(Pubkey::from(TREASURY_ADDRESS)).into(),
        supply,
        decimals: TOKEN_DECIMALS,
        is_initialized: true,
        freeze_authority: None.into(),
    };

    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_data, &mut data).unwrap();

    (MINT_ADDRESS.into(), Account {
        lamports: rent_mint(),
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

const TOKEN_ELF : &[u8] = program_elf!("../target/deploy/token.so");
pub fn tape_program() -> (Pubkey, Account) {
    let account = mollusk_svm::program::create_program_account_loader_v2(TOKEN_ELF);
    (program::token::ID, account)
}

const EXCHANGE_ELF : &[u8] = program_elf!("../target/deploy/exchange.so");
pub fn exchange_program() -> (Pubkey, Account) {
    let account = mollusk_svm::program::create_program_account_loader_v2(EXCHANGE_ELF);
    (program::exchange::ID, account)
}

const STAKING_ELF : &[u8] = program_elf!("../target/deploy/staking.so");
pub fn staking_program() -> (Pubkey, Account) {
    let account = mollusk_svm::program::create_program_account_loader_v2(STAKING_ELF);
    (program::staking::ID, account)
}

const TAPEDRIVE_ELF : &[u8] = program_elf!("../target/deploy/tapedrive.so");
pub fn tapedrive_program() -> (Pubkey, Account) {
    let account = mollusk_svm::program::create_program_account_loader_v2(TAPEDRIVE_ELF);
    (program::tapedrive::ID, account)
}

pub fn with_programs(programs: &[(&Pubkey, &'static [u8])]) -> TestEnv {
    let mut mollusk = Mollusk::default();

    mollusk.add_program_with_elf_and_loader(&program::exchange::ID, EXCHANGE_ELF, &DEFAULT_LOADER_KEY);
    mollusk.add_program_with_elf_and_loader(&program::staking::ID, STAKING_ELF, &DEFAULT_LOADER_KEY);
    mollusk.add_program_with_elf_and_loader(&program::tapedrive::ID, TAPEDRIVE_ELF, &DEFAULT_LOADER_KEY);
    mollusk.add_program_with_elf_and_loader(&program::token::ID, TOKEN_ELF, &DEFAULT_LOADER_KEY);

    mollusk.logger = Some(solana_log_collector::LogCollector::new_ref());

    spl_token_program::add_program(&mut mollusk);
    spl_ata_program::add_program(&mut mollusk);

    for (id, elf) in programs {
        mollusk.add_program_with_elf_and_loader(id, elf, &LOADER_V2);
    }

    TestEnv { mollusk }
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

        result.run_checks(checks, &CheckConfig::default(), &self.mollusk);
    }

    fn print_instruction(&self, instruction: &Instruction) {
        println!("\n");
        println!("--------------------------------------------------------------------------------");
        println!("Program: {}", instruction.program_id);

        if !instruction.data.is_empty() {
            let discriminator = instruction.data[0];

            let ix_type = if let Ok(instruction) = TapeInstruction::try_from(discriminator) {
                format!("TapeInstruction::{:?}", instruction)
            } else if let Ok(instruction) = StakingInstruction::try_from(discriminator) {
                format!("StakingInstruction::{:?}", instruction)
            } else if let Ok(instruction) = ExchangeInstruction::try_from(discriminator) {
                format!("ExchangeInstruction::{:?}", instruction)
            } else if let Ok(instruction) = TokenInstruction::try_from(discriminator) {
                format!("TokenInstruction::{:?}", instruction)
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

    pub fn now(&self) -> i64 {
        self.mollusk.sysvars.clock.unix_timestamp
    }

    pub fn slot(&self) -> u64 {
        self.mollusk.sysvars.clock.slot
    }
}


pub fn test_env() -> TestEnv {
    with_programs(&[
        (&mpl_token_metadata::ID, METAPLEX_ELF),
    ])
}

// ELF helpers

#[macro_export]
macro_rules! program_elf {
    ($relative_path:literal) => {
        include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $relative_path))
    };
}
