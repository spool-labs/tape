#![allow(unexpected_cfgs)]

pub mod archive;
pub mod blob;
pub mod committee;
pub mod epoch;
pub mod exchange;
pub mod node;
pub mod stake;
pub mod system;
pub mod tape;

use archive::*;
use blob::*;
use committee::*;
use epoch::*;
use exchange::*;
use node::*;
use stake::*;
use system::*;
use tape::*;

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

            TapeInstruction::CreateArchive => process_create_archive(accounts, data)?,

            TapeInstruction::CreateEpoch => process_create_epoch(accounts, data)?,
            TapeInstruction::ExpandEpoch => process_expand_epoch(accounts, data)?,
            TapeInstruction::AdvanceEpoch => process_advance_epoch(accounts, data)?,

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

            // Node
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::JoinNetwork => process_join_network(accounts, data)?,

            // Stake
            TapeInstruction::StakeWithNode => process_stake_with_node(accounts, data)?,

            // Tape
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
