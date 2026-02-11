#![allow(unexpected_cfgs)]

pub mod archive;
pub mod blacklist;
pub mod epoch;
pub mod error;
pub mod node;
pub mod snapshot;
pub mod staking;
pub mod tape;
pub mod track;

use archive::*;
use blacklist::*;
use epoch::*;
use node::*;
use snapshot::*;
use staking::*;
use tape::*;
use track::*;

use tape_solana::*;
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
            TapeInstruction::ExpandSystem => process_expand_system(accounts, data)?,
            TapeInstruction::Initialize => process_initialize(accounts, data)?,
            TapeInstruction::AdvanceEpoch => process_advance_epoch(accounts, data)?,

            // Node
            TapeInstruction::AdvancePool => process_advance_pool(accounts, data)?,
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::JoinNetwork => process_join_network(accounts, data)?,
            TapeInstruction::SyncEpoch => process_sync_epoch(accounts, data)?,
            TapeInstruction::SetAuthority => process_set_authority(accounts, data)?,
            TapeInstruction::SetBlsPubkey => process_set_bls_pubkey(accounts, data)?,
            TapeInstruction::SetName => process_set_name(accounts, data)?,
            TapeInstruction::SetNetworkAddress => process_set_network_address(accounts, data)?,
            TapeInstruction::SetNetworkTls => process_set_network_tls(accounts, data)?,
            TapeInstruction::SetStoragePrice => process_set_storage_price(accounts, data)?,
            TapeInstruction::SetStorageCapacity => process_set_storage_capacity(accounts, data)?,
            TapeInstruction::SetCommissionRate => process_set_commission_rate(accounts, data)?,
            TapeInstruction::ClaimCommission => process_claim_commission(accounts, data)?,

            // Blacklist
            TapeInstruction::AddToBlacklist => process_add_to_blacklist(accounts, data)?,
            TapeInstruction::RemoveFromBlacklist => process_remove_from_blacklist(accounts, data)?,
 
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
            TapeInstruction::CertifyTrack => process_certify_track(accounts, data)?,
            TapeInstruction::InvalidateTrack => process_invalidate_track(accounts, data)?,

            // Snapshot
            TapeInstruction::ReserveSnapshotTape => process_reserve_snapshot_tape(accounts, data)?,
            TapeInstruction::RegisterSnapshot => process_register_snapshot(accounts, data)?,
            TapeInstruction::CertifySnapshot => process_certify_snapshot(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);

