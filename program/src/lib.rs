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
    let (discriminator, data) = parse_instruction(&tape_api::ID, program_id, data)?;

    if let Ok(ix) = ProgramInstruction::try_from_primitive(discriminator) {
        match ix {
            ProgramInstruction::Initialize => process_initialize(accounts, data)?,
            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else if let Ok(ix) = TapeInstruction::try_from_primitive(discriminator) {
        match ix {
            TapeInstruction::Create => process_create(accounts, data)?,
            TapeInstruction::Write => process_write(accounts, data)?,
            TapeInstruction::Update => process_update(accounts, data)?,
            TapeInstruction::Finalize => process_finalize(accounts, data)?,
            TapeInstruction::SetHeader => process_set_header(accounts, data)?,
            TapeInstruction::Subsidize => process_subsidize_rent(accounts, data)?,
        }
    } else if let Ok(ix) = MinerInstruction::try_from_primitive(discriminator) {
        match ix {
            MinerInstruction::Register => process_register(accounts, data)?,
            MinerInstruction::Unregister => process_unregister(accounts, data)?,
            MinerInstruction::Mine => process_mine(accounts, data)?,
            MinerInstruction::Claim => process_claim(accounts, data)?,
        }
    // } else if let Ok(ix) = BinInstruction::try_from_primitive(discriminator) {
    //     match ix {
    //         BinInstruction::Create => process_bin_create(accounts, data)?,
    //         BinInstruction::Destroy => process_bin_destroy(accounts, data)?,
    //         BinInstruction::Pack => process_bin_pack(accounts, data)?,
    //         BinInstruction::Unpack => process_bin_unpack(accounts, data)?,
    //     }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
