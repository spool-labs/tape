use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};
use solana_program::program_error::ProgramError;

use tape_core::bls::BlsSignature;
use tape_core::cert::{AssignmentGroupPayload, ASSIGNMENT_TREE_HEIGHT};
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_crypto::address::Address;
use tape_crypto::Hash;

use crate::program::tapedrive;
use crate::program::tapedrive::{
    assignment_vote_pda, committee_pda, epoch_pda, group_pda, peer_set_pda,
    snapshot_tape_pda, snapshot_vote_pda, system_pda,
};
use crate::helpers::read_instruction_pod;
use crate::state::Tape;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ProposeSnapshot {
    pub hash: Hash,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteSnapshot {
    pub hash: Hash,
    pub group: [u8; 8],
    pub bitmap: SpoolBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeSnapshot {
    pub epoch: [u8; 8],
    pub tape: Tape,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ProposeAssignment {
    pub hash: Hash,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteAssignment {
    pub hash: Hash,
    pub group: [u8; 8],
    pub bitmap: SpoolBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeGroup {
    pub epoch: [u8; 8],
    pub payload: AssignmentGroupPayload,
    pub proof: [Hash; ASSIGNMENT_TREE_HEIGHT],
}

#[inline(always)]
pub fn parse_propose_snapshot(data: &[u8]) -> Result<ProposeSnapshot, ProgramError> {
    read_instruction_pod::<ProposeSnapshot>(data)
}

#[inline(always)]
pub fn parse_vote_snapshot(data: &[u8]) -> Result<VoteSnapshot, ProgramError> {
    read_instruction_pod::<VoteSnapshot>(data)
}

#[inline(always)]
pub fn parse_finalize_snapshot(data: &[u8]) -> Result<FinalizeSnapshot, ProgramError> {
    read_instruction_pod::<FinalizeSnapshot>(data)
}

#[inline(always)]
pub fn parse_propose_assignment(data: &[u8]) -> Result<ProposeAssignment, ProgramError> {
    read_instruction_pod::<ProposeAssignment>(data)
}

#[inline(always)]
pub fn parse_vote_assignment(data: &[u8]) -> Result<VoteAssignment, ProgramError> {
    read_instruction_pod::<VoteAssignment>(data)
}

#[inline(always)]
pub fn parse_finalize_group(data: &[u8]) -> Result<FinalizeGroup, ProgramError> {
    read_instruction_pod::<FinalizeGroup>(data)
}

pub fn build_propose_snapshot_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
    hash: Hash,
) -> Instruction {
    let target_epoch = current_epoch.saturating_sub(EpochNumber(1));
    let (system_address, _) = system_pda();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (vote_address, _) = snapshot_vote_pda(current_epoch, target_epoch, hash);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(curr_epoch_address.into(), false),
            AccountMeta::new_readonly(target_epoch_address.into(), false),
            AccountMeta::new(vote_address.into(), false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: ProposeSnapshot { hash }.to_bytes(),
    }
}

pub fn build_vote_snapshot_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
    hash: Hash,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Instruction {
    let target_epoch = current_epoch.saturating_sub(EpochNumber(1));
    let (system_address, _) = system_pda();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (curr_group_address, _) = group_pda(current_epoch, group);
    let (vote_address, _) = snapshot_vote_pda(current_epoch, target_epoch, hash);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(curr_epoch_address.into(), false),
            AccountMeta::new(target_epoch_address.into(), false),
            AccountMeta::new_readonly(curr_group_address.into(), false),
            AccountMeta::new(vote_address.into(), false),
        ],
        data: VoteSnapshot {
            hash,
            group: group.pack(),
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}

pub fn build_finalize_snapshot_ix(
    fee_payer: Address,
    epoch: EpochNumber,
    tape: Tape,
) -> Instruction {
    let (epoch_address, _) = epoch_pda(epoch);
    let (snapshot_tape_address, _) = snapshot_tape_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(snapshot_tape_address.into(), false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: FinalizeSnapshot {
            epoch: epoch.pack(),
            tape,
        }
        .to_bytes(),
    }
}

pub fn build_propose_assignment_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
    hash: Hash,
) -> Instruction {
    let target_epoch = current_epoch.saturating_add(EpochNumber(1));
    let (system_address, _) = system_pda();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (vote_address, _) = assignment_vote_pda(current_epoch, target_epoch, hash);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(curr_epoch_address.into(), false),
            AccountMeta::new_readonly(target_epoch_address.into(), false),
            AccountMeta::new(vote_address.into(), false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: ProposeAssignment { hash }.to_bytes(),
    }
}

pub fn build_vote_assignment_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
    hash: Hash,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Instruction {
    let target_epoch = current_epoch.saturating_add(EpochNumber(1));
    let (system_address, _) = system_pda();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (curr_group_address, _) = group_pda(current_epoch, group);
    let (vote_address, _) = assignment_vote_pda(current_epoch, target_epoch, hash);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(curr_epoch_address.into(), false),
            AccountMeta::new(target_epoch_address.into(), false),
            AccountMeta::new_readonly(curr_group_address.into(), false),
            AccountMeta::new(vote_address.into(), false),
        ],
        data: VoteAssignment {
            hash,
            group: group.pack(),
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}

pub fn build_finalize_group_ix(
    fee_payer: Address,
    epoch: EpochNumber,
    payload: AssignmentGroupPayload,
    proof: [Hash; ASSIGNMENT_TREE_HEIGHT],
) -> Instruction {
    let group = payload.group();
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda(epoch);
    let (group_address, _) = group_pda(epoch, group);
    let (committee_address, _) = committee_pda(epoch);
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(epoch_address.into(), false),
            AccountMeta::new(group_address.into(), false),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new_readonly(peer_set_address.into(), false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: FinalizeGroup {
            epoch: epoch.pack(),
            payload,
            proof,
        }
        .to_bytes(),
    }
}
