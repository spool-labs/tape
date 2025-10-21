#![allow(unexpected_cfgs)]

pub mod initialize;
use initialize::*;

use tape_api::prelude::*;
use tape_api::program::token;
use steel::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&token::ID, program_id, data)?;

    let ix_type = if let Ok(instruction) = TokenInstruction::try_from_primitive(discriminator) {
        format!("{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = TokenInstruction::try_from(discriminator) {
        match ix {

            TokenInstruction::InitializeMint => process_initialize_mint(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);

