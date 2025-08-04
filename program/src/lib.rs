#![allow(unexpected_cfgs)]

pub mod tape;
pub mod miner;
pub mod spool;
pub mod program;

use tape::*;
use miner::*;
use spool::*;
use program::*;

use tape_api::instruction::{
    tape::TapeInstruction,
    miner::MinerInstruction,
    program::ProgramInstruction,
    spool::SpoolInstruction,
};
use steel::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&tape_api::ID, program_id, data)?;

    let ix_type = if let Ok(instruction) = ProgramInstruction::try_from_primitive(discriminator) {
        format!("ProgramInstruction::{:?}", instruction)
    } else if let Ok(instruction) = TapeInstruction::try_from_primitive(discriminator) {
        format!("TapeInstruction::{:?}", instruction)
    } else if let Ok(instruction) = MinerInstruction::try_from_primitive(discriminator) {
        format!("MinerInstruction::{:?}", instruction)
    } else if let Ok(instruction) = SpoolInstruction::try_from_primitive(discriminator) {
        format!("SpoolInstruction::{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = ProgramInstruction::try_from_primitive(discriminator) {
        match ix {
            ProgramInstruction::Initialize => process_initialize(accounts, data)?,
            #[cfg(feature = "airdrop")]
            ProgramInstruction::Airdrop => process_airdrop(accounts, data)?,
            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else if let Ok(ix) = TapeInstruction::try_from_primitive(discriminator) {
        match ix {
            TapeInstruction::Create => process_tape_create(accounts, data)?,
            TapeInstruction::Write => process_tape_write(accounts, data)?,
            TapeInstruction::Update => process_tape_update(accounts, data)?,
            TapeInstruction::Finalize => process_tape_finalize(accounts, data)?,
            TapeInstruction::SetHeader => process_tape_set_header(accounts, data)?,
            TapeInstruction::Subsidize => process_tape_subsidize_rent(accounts, data)?,
        }
    } else if let Ok(ix) = MinerInstruction::try_from_primitive(discriminator) {
        match ix {
            MinerInstruction::Register => process_register(accounts, data)?,
            MinerInstruction::Unregister => process_unregister(accounts, data)?,
            MinerInstruction::Mine => process_mine(accounts, data)?,
            MinerInstruction::Claim => process_claim(accounts, data)?,
        }
     } else if let Ok(ix) = SpoolInstruction::try_from_primitive(discriminator) {
         match ix {
            SpoolInstruction::Create => process_spool_create(accounts, data)?,
            SpoolInstruction::Destroy => process_spool_destroy(accounts, data)?,
            SpoolInstruction::Pack => process_spool_pack(accounts, data)?,
            SpoolInstruction::Unpack => process_spool_unpack(accounts, data)?,
            SpoolInstruction::Commit => process_spool_commit(accounts, data)?,
         }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
