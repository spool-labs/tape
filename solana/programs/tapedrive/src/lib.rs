#![allow(unexpected_cfgs)]

pub mod archive;
pub mod blacklist;
pub mod committee;
pub mod epoch;
pub mod error;
pub mod node;
pub mod peer;
pub mod pool;
pub mod system;
pub mod tape;
pub mod track;
pub mod vote;

use tape_api::program::prelude::{TapeInstruction, parse_instruction};
use tape_api::program::tapedrive;
use tape_solana::{AccountInfo, ProgramError, ProgramResult, Pubkey, TryFromPrimitive, entrypoint};

use crate::archive::process_create_archive;
use crate::system::{
    process_create_system,
    process_stage_genesis_node,
    process_start_network,
};
use crate::blacklist::{process_add_to_blacklist, process_remove_from_blacklist};
use crate::committee::{
    process_create_committee,
    process_resize_committee,
};
use crate::epoch::{
    process_advance_epoch,
    process_commit_epoch,
    process_create_epoch,
    process_sync_spool,
};
use crate::node::{
    process_claim_commission,
    process_join_committee,
    process_register_node,
    process_set_authority,
    process_set_bls_pubkey,
    process_set_burn_fee_bps,
    process_set_commission_rate,
    process_set_committee_size,
    process_set_epoch_duration,
    process_set_access_threshold,
    process_set_name,
    process_set_network_address,
    process_set_network_tls,
    process_set_spool_groups,
    process_set_storage_capacity,
    process_set_storage_price,
    process_set_subsidy_decay_bps,
};
use crate::peer::{process_create_peer_set, process_resize_peer_set};
use crate::pool::{
    process_advance_pool,
    process_merge_pool_stake,
    process_request_stake_unlock,
    process_split_pool_stake,
    process_stake_with_pool,
    process_unstake_from_pool,
};
use crate::tape::{
    process_destroy_tape,
    process_extend_tape_capacity,
    process_extend_tape_expiry,
    process_revoke_tape_delegate,
    process_reserve_tape,
    process_set_tape_delegate,
};
use crate::track::{
    process_certify_track,
    process_delete_track,
    process_invalidate_track,
    process_track_write,
};
use crate::vote::{
    process_finalize_group,
    process_finalize_snapshot,
    process_propose_assignment,
    process_propose_eviction,
    process_propose_snapshot,
    process_vote_assignment,
    process_vote_eviction,
    process_vote_snapshot,
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

    let ix = TapeInstruction::try_from(discriminator)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match ix {
        TapeInstruction::Unknown => return Err(ProgramError::InvalidInstructionData),

        // System
        TapeInstruction::CreateSystem => process_create_system(accounts, data)?,
        TapeInstruction::CreateArchive => process_create_archive(accounts, data)?,
        TapeInstruction::CreateCommittee => process_create_committee(accounts, data)?,
        TapeInstruction::CreateEpoch => process_create_epoch(accounts, data)?,
        TapeInstruction::CreatePeerSet => process_create_peer_set(accounts, data)?,
        TapeInstruction::ResizeCommittee => process_resize_committee(accounts, data)?,
        TapeInstruction::ResizePeerSet => process_resize_peer_set(accounts, data)?,
        TapeInstruction::StageGenesisNode => process_stage_genesis_node(accounts, data)?,
        TapeInstruction::StartNetwork => process_start_network(accounts, data)?,

        // Epoch
        TapeInstruction::SyncSpool => process_sync_spool(accounts, data)?,
        TapeInstruction::CommitEpoch => process_commit_epoch(accounts, data)?,
        TapeInstruction::AdvanceEpoch => process_advance_epoch(accounts, data)?,

        // Operator
        TapeInstruction::RegisterNode => process_register_node(accounts, data)?,
        TapeInstruction::JoinCommittee => process_join_committee(accounts, data)?,
        TapeInstruction::SetAuthority => process_set_authority(accounts, data)?,
        TapeInstruction::SetName => process_set_name(accounts, data)?,
        TapeInstruction::SetBlsPubkey => process_set_bls_pubkey(accounts, data)?,
        TapeInstruction::SetNetworkAddress => process_set_network_address(accounts, data)?,
        TapeInstruction::SetNetworkTls => process_set_network_tls(accounts, data)?,
        TapeInstruction::SetCommissionRate => process_set_commission_rate(accounts, data)?,
        TapeInstruction::SetStoragePrice => process_set_storage_price(accounts, data)?,
        TapeInstruction::SetBurnFeeBps => process_set_burn_fee_bps(accounts, data)?,
        TapeInstruction::SetSubsidyDecayBps => process_set_subsidy_decay_bps(accounts, data)?,
        TapeInstruction::SetStorageCapacity => process_set_storage_capacity(accounts, data)?,
        TapeInstruction::SetAccessThreshold => process_set_access_threshold(accounts, data)?,
        TapeInstruction::SetCommitteeSize => process_set_committee_size(accounts, data)?,
        TapeInstruction::SetSpoolGroups => process_set_spool_groups(accounts, data)?,
        TapeInstruction::SetEpochDuration => process_set_epoch_duration(accounts, data)?,
        TapeInstruction::ClaimCommission => process_claim_commission(accounts, data)?,
        TapeInstruction::AddToBlacklist => process_add_to_blacklist(accounts, data)?,
        TapeInstruction::RemoveFromBlacklist => process_remove_from_blacklist(accounts, data)?,

        // Pool
        TapeInstruction::AdvancePool => process_advance_pool(accounts, data)?,
        TapeInstruction::StakeWithPool => process_stake_with_pool(accounts, data)?,
        TapeInstruction::RequestStakeUnlock => process_request_stake_unlock(accounts, data)?,
        TapeInstruction::UnstakeFromPool => process_unstake_from_pool(accounts, data)?,
        TapeInstruction::SplitPoolStake => process_split_pool_stake(accounts, data)?,
        TapeInstruction::MergePoolStake => process_merge_pool_stake(accounts, data)?,

        // Tape
        TapeInstruction::ReserveTape => process_reserve_tape(accounts, data)?,
        TapeInstruction::DestroyTape => process_destroy_tape(accounts, data)?,
        TapeInstruction::ExtendTapeCapacity => process_extend_tape_capacity(accounts, data)?,
        TapeInstruction::ExtendTapeExpiry => process_extend_tape_expiry(accounts, data)?,
        TapeInstruction::SetTapeDelegate => process_set_tape_delegate(accounts, data)?,
        TapeInstruction::RevokeTapeDelegate => process_revoke_tape_delegate(accounts, data)?,

        // Track
        TapeInstruction::TrackWrite => process_track_write(accounts, data)?,
        TapeInstruction::DeleteTrack => process_delete_track(accounts, data)?,
        TapeInstruction::CertifyTrack => process_certify_track(accounts, data)?,
        TapeInstruction::InvalidateTrack => process_invalidate_track(accounts, data)?,

        // Vote
        TapeInstruction::ProposeSnapshot => process_propose_snapshot(accounts, data)?,
        TapeInstruction::VoteSnapshot => process_vote_snapshot(accounts, data)?,
        TapeInstruction::FinalizeSnapshot => process_finalize_snapshot(accounts, data)?,
        TapeInstruction::ProposeAssignment => process_propose_assignment(accounts, data)?,
        TapeInstruction::VoteAssignment => process_vote_assignment(accounts, data)?,
        TapeInstruction::FinalizeGroup => process_finalize_group(accounts, data)?,
        TapeInstruction::ProposeEviction => process_propose_eviction(accounts, data)?,
        TapeInstruction::VoteEviction => process_vote_eviction(accounts, data)?,
    }

    Ok(())
}

entrypoint!(process_instruction);
