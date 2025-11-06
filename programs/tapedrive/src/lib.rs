#![allow(unexpected_cfgs)]

pub mod archive;
pub mod epoch;
pub mod error;
pub mod node;
pub mod staking;
pub mod system;
pub mod tape;
pub mod track;

use archive::*;
use epoch::*;
use node::*;
use staking::*;
use system::*;
use tape::*;
use track::*;

use steel::*;
use tape_api::prelude::*;
use tape_api::program::tapedrive;

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
            TapeInstruction::AdvancePool => process_advance_pool(accounts, data)?,
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::JoinNetwork => process_join_network(accounts, data)?,

            // Staking
            TapeInstruction::StakeWithPool => process_stake_with_pool(accounts, data)?,
            TapeInstruction::RequestStakeUnlock => process_request_stake_unlock(accounts, data)?,
            TapeInstruction::UnstakeFromPool => process_unstake_from_pool(accounts, data)?,
            TapeInstruction::MergePoolStake => process_merge_pool_stake(accounts, data)?,
            TapeInstruction::SplitPoolStake => process_split_pool_stake(accounts, data)?,

            // Tape
            TapeInstruction::ReserveTape => process_reserve_tape(accounts, data)?,
            TapeInstruction::DestroyTape => process_destroy_tape(accounts, data)?,
            TapeInstruction::SplitTapeByEpoch => process_split_tape_by_epoch(accounts, data)?,
            TapeInstruction::SplitTapeBySize => process_split_tape_by_size(accounts, data)?,
            TapeInstruction::MergeTape => process_merge_tape(accounts, data)?,

            // Track
            TapeInstruction::RegisterTrack => process_register_track(accounts, data)?,
            TapeInstruction::DeleteTrack => process_delete_track(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);

