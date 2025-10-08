#![allow(unexpected_cfgs)]

pub mod blob;
pub mod committee;
pub mod epoch;
pub mod exchange;
pub mod operator;
pub mod staking;
pub mod storage;
pub mod system;

use blob::*;
use committee::*;
use epoch::*;
use exchange::*;
use operator::*;
use staking::*;
use storage::*;
use system::*;

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
            // System
            TapeInstruction::Initialize => process_initialize(accounts, data)?,

            TapeInstruction::CreateEpoch => process_create_epoch(accounts, data)?,
            TapeInstruction::ExpandEpoch => process_expand_epoch(accounts, data)?,

            TapeInstruction::CreateCommittee => process_create_committee(accounts, data)?,
            TapeInstruction::ExpandCommittee => process_expand_committee(accounts, data)?,

            // Exchange
            TapeInstruction::RegisterExchange => process_register_exchange(accounts, data)?,
            TapeInstruction::SetExchangeRate => process_set_exchange_rate(accounts, data)?,
            TapeInstruction::DepositSol => process_deposit_sol(accounts, data)?,
            TapeInstruction::DepositTape => process_deposit_tape(accounts, data)?,
            TapeInstruction::WithdrawSol => process_withdraw_sol(accounts, data)?,
            TapeInstruction::WithdrawTape => process_withdraw_tape(accounts, data)?,
            TapeInstruction::SwapForTape => process_swap_for_tape(accounts, data)?,
            TapeInstruction::SwapForSol => process_swap_for_sol(accounts, data)?,

            // Operator
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,

            // Staking
            TapeInstruction::StakeWithNode => process_stake_with_node(accounts, data)?,

            // Storage
            TapeInstruction::ReserveTape => process_reserve_tape(accounts, data)?,
            
            // Blob
            
            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
