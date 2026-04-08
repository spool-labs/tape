#![allow(unexpected_cfgs)]

pub mod stake;
pub mod unstake;
pub mod split;
pub mod merge;

use stake::*;
use unstake::*;
use split::*;
use merge::*;

use tape_api::program::prelude::*;
use tape_api::program::staking;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&staking::ID, program_id, data)?;

    solana_program::msg!("Staking Program ID: {}", staking::id());


    let ix_type = if let Ok(instruction) = StakingInstruction::try_from_primitive(discriminator) {
        format!("{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = StakingInstruction::try_from_primitive(discriminator) {
        match ix {

            StakingInstruction::StakeTokens => process_stake_tokens(accounts, data)?,
            StakingInstruction::UnstakeTokens => process_unstake_tokens(accounts, data)?,
            StakingInstruction::SplitStake => process_split_stake(accounts, data)?,
            StakingInstruction::MergeStake => process_merge_stake(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
