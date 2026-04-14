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

use tape_api::program::prelude::{TapeInstruction, parse_instruction};
use tape_api::program::tapedrive;
use tape_solana::{AccountInfo, ProgramError, ProgramResult, Pubkey, TryFromPrimitive, entrypoint};

use crate::archive::{
    process_create_system, 
    process_expand_system, 
    process_initialize
};
use crate::blacklist::{
    process_add_to_blacklist, 
    process_remove_from_blacklist
};
use crate::epoch::process_advance_epoch;
use crate::node::{
    process_advance_pool, process_claim_commission, process_join_network,
    process_register_node, process_set_authority, process_set_bls_pubkey,
    process_set_commission_rate, process_set_name, process_set_network_address,
    process_set_network_tls, process_set_storage_capacity, process_set_storage_price,
    process_sync_epoch,
};
use crate::snapshot::{
    process_reserve_snapshot, 
    process_write_snapshot,
    process_sign_snapshot,
};
use crate::staking::{
    process_merge_pool_stake, process_request_stake_unlock, process_split_pool_stake,
    process_stake_with_pool, process_unstake_from_pool,
};
use crate::tape::{
    process_destroy_tape, process_merge_tape, process_reserve_tape,
    process_split_tape_by_epoch, process_split_tape_by_size,
};
use crate::track::{
    process_certify_track, process_delete_track, process_invalidate_track,
    process_track_write,
};

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

    // Instructions marked with CU require a ComputeBudgetInstruction in the
    // transaction.  See `tape_api::compute` for the canonical budget constants.
    if let Ok(ix) = TapeInstruction::try_from(discriminator) {
        match ix {

            // System
            TapeInstruction::CreateSystem => process_create_system(accounts, data)?,
            TapeInstruction::ExpandSystem => process_expand_system(accounts, data)?,
            TapeInstruction::Initialize => process_initialize(accounts, data)?,
            TapeInstruction::AdvanceEpoch => process_advance_epoch(accounts, data)?,    // CU 1_400_000

            // Node
            TapeInstruction::AdvancePool => process_advance_pool(accounts, data)?,      // CU   400_000
            TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
            TapeInstruction::JoinNetwork => process_join_network(accounts, data)?,      // CU   400_000
            TapeInstruction::SyncEpoch => process_sync_epoch(accounts, data)?,          // CU   400_000
            TapeInstruction::SetAuthority => process_set_authority(accounts, data)?,
            TapeInstruction::SetBlsPubkey => process_set_bls_pubkey(accounts, data)?,
            TapeInstruction::SetName => process_set_name(accounts, data)?,
            TapeInstruction::SetNetworkAddress => process_set_network_address(accounts, data)?,
            TapeInstruction::SetNetworkTls => process_set_network_tls(accounts, data)?,
            TapeInstruction::SetStoragePrice => process_set_storage_price(accounts, data)?,
            TapeInstruction::SetStorageCapacity => process_set_storage_capacity(accounts, data)?,
            TapeInstruction::SetCommissionRate => process_set_commission_rate(accounts, data)?,  // CU 400_000
            TapeInstruction::ClaimCommission => process_claim_commission(accounts, data)?,       // CU 400_000

            // Blacklist
            TapeInstruction::AddToBlacklist => process_add_to_blacklist(accounts, data)?,
            TapeInstruction::RemoveFromBlacklist => process_remove_from_blacklist(accounts, data)?,

            // Staking
            TapeInstruction::StakeWithPool => process_stake_with_pool(accounts, data)?,             // CU 1_400_000
            TapeInstruction::RequestStakeUnlock => process_request_stake_unlock(accounts, data)?,   // CU   400_000
            TapeInstruction::UnstakeFromPool => process_unstake_from_pool(accounts, data)?,         // CU   400_000
            TapeInstruction::MergePoolStake => process_merge_pool_stake(accounts, data)?,
            TapeInstruction::SplitPoolStake => process_split_pool_stake(accounts, data)?,

            // Tape
            TapeInstruction::ReserveTape => process_reserve_tape(accounts, data)?,
            TapeInstruction::DestroyTape => process_destroy_tape(accounts, data)?,
            TapeInstruction::SplitTapeByEpoch => process_split_tape_by_epoch(accounts, data)?,
            TapeInstruction::SplitTapeBySize => process_split_tape_by_size(accounts, data)?,
            TapeInstruction::MergeTape => process_merge_tape(accounts, data)?,

            // Track
            TapeInstruction::TrackWrite => process_track_write(accounts, data)?,
            TapeInstruction::DeleteTrack => process_delete_track(accounts, data)?,
            TapeInstruction::CertifyTrack => process_certify_track(accounts, data)?,        // CU 1_400_000
            TapeInstruction::InvalidateTrack => process_invalidate_track(accounts, data)?,  // CU 1_400_000

            // Snapshot
            TapeInstruction::ReserveSnapshot => process_reserve_snapshot(accounts, data)?,
            TapeInstruction::WriteSnapshot => process_write_snapshot(accounts, data)?,
            TapeInstruction::SignSnapshot => process_sign_snapshot(accounts, data)?,

            _ => return Err(ProgramError::InvalidInstructionData),
        }
    } else {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

entrypoint!(process_instruction);
