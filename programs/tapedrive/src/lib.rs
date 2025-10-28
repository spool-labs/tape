#![allow(unexpected_cfgs)]

pub mod error;
pub mod archive;
//pub mod blob;
//pub mod committee;
pub mod epoch;
//pub mod exchange;
pub mod node;
pub mod staking;
pub mod system;
//pub mod tape;

use archive::*;
//use blob::*;
//use committee::*;
use epoch::*;
//use exchange::*;
use node::*;
use staking::*;
use system::*;
//use tape::*;

use tape_api::prelude::*;
use tape_api::program::tapedrive;
use steel::*;

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    data: &[u8],
) -> ProgramResult {
    let (discriminator, data) = parse_instruction(&tapedrive::ID, program_id, data)?;

    let ix_type = if let Ok(instruction) = TapeInstruction::try_from_primitive(discriminator) {
        format!("{:?}", instruction)
    } else {
        format!("Invalid (discriminator: {})", discriminator)
    };

    solana_program::msg!("Instruction: {}", ix_type);

    if let Ok(ix) = TapeInstruction::try_from(discriminator) {
        match ix {

            // System
            TapeInstruction::CreateSystem => process_create_system(accounts, data)?,
            TapeInstruction::CreateArchive => process_create_archive(accounts, data)?,
            TapeInstruction::CreateEpoch => process_create_epoch(accounts, data)?,

            TapeInstruction::ExpandSystem => process_expand_system(accounts, data)?,
            TapeInstruction::Initialize => process_initialize(accounts, data)?,

            TapeInstruction::AdvanceEpoch => process_advance_epoch(accounts, data)?,
            TapeInstruction::SyncEpoch => process_sync_epoch(accounts, data)?,

            // Node
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::JoinNetwork => process_join_network(accounts, data)?,

            // Staking
            TapeInstruction::StakeWithPool => process_stake_with_pool(accounts, data)?,
            //TapeInstruction::RequestStakeUnlock => process_request_stake_unlock(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);

