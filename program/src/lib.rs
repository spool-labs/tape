#![allow(unexpected_cfgs)]

pub mod tape;
pub mod miner;
pub mod program;

use tape::*;
use miner::*;
use program::*;

use tape_api::instruction::*;
use steel::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (ix, data) = parse_instruction(&tape_api::ID, program_id, data)?;

    match ix {
        // Program instructions
        InstructionType::Initialize => process_initialize(accounts, data)?,

        // Tape instructions
        InstructionType::Create => process_create(accounts, data)?,
        InstructionType::Write => process_write(accounts, data)?,
        InstructionType::Update => process_update(accounts, data)?,
        InstructionType::Finalize => process_finalize(accounts, data)?,

        // Miner instructions
        InstructionType::Register => process_register(accounts, data)?,
        InstructionType::Close => process_close(accounts, data)?,
        InstructionType::Mine => process_mine(accounts, data)?,
        InstructionType::Claim => process_claim(accounts, data)?,

        _ => { return Err(ProgramError::InvalidInstructionData); }
    }

    Ok(())
}

entrypoint!(process_instruction);
