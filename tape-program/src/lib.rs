#![allow(unexpected_cfgs)]

pub mod program;
pub mod staking;
pub mod data;

use program::*;
use staking::*;
use data::*;

use tape_api::prelude::*;
use steel::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&tape_api::ID, program_id, data)?;

    let ix_type = if let Ok(instruction) = TapeInstruction::try_from_primitive(discriminator) {
        format!("{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = TapeInstruction::try_from_primitive(discriminator) {
        match ix {
            TapeInstruction::Initialize => process_initialize(accounts, data)?,
            TapeInstruction::Airdrop => process_airdrop(accounts, data)?,
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::Stake => process_stake(accounts, data)?,
            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
