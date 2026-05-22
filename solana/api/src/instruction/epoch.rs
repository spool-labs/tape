use tape_core::types::EpochNumber;
use tape_solana::*;
use tape_crypto::address::Address;
use crate::program::tapedrive;
use crate::program::tapedrive::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateEpoch {
    pub epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CommitEpoch {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}

pub fn build_create_epoch_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (epoch_address, _) = epoch_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(epoch_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateEpoch { epoch: epoch.pack() }.to_bytes(),
    }
}

pub fn build_commit_epoch_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
) -> Instruction {
    let next_epoch = current_epoch.saturating_add(EpochNumber(1));
    let prev_epoch = current_epoch.saturating_sub(EpochNumber(1));

    let (system_address, _) = system_pda();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (next_epoch_address, _) = epoch_pda(next_epoch);
    let (curr_committee_address, _) = committee_pda(current_epoch);
    let (next_committee_address, _) = committee_pda(next_epoch);
    let (peer_set_address, _) = peer_set_pda();
    let (snapshot_tape_address, _) = snapshot_tape_pda(prev_epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(curr_epoch_address.into(), false),
            AccountMeta::new(next_epoch_address.into(), false),
            AccountMeta::new_readonly(curr_committee_address.into(), false),
            AccountMeta::new_readonly(next_committee_address.into(), false),
            AccountMeta::new_readonly(peer_set_address.into(), false),
            AccountMeta::new_readonly(snapshot_tape_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: CommitEpoch {}.to_bytes(),
    }
}

pub fn build_advance_epoch_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
) -> Instruction {
    let next_epoch = current_epoch.saturating_add(EpochNumber(1));
    let target_epoch = next_epoch.saturating_add(EpochNumber(1));

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (current_epoch_address, _) = epoch_pda(current_epoch);
    let (next_epoch_address, _) = epoch_pda(next_epoch);
    let (next_committee_address, _) = committee_pda(next_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (target_committee_address, _) = committee_pda(target_epoch);
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(current_epoch_address.into(), false),
            AccountMeta::new(next_epoch_address.into(), false),
            AccountMeta::new(next_committee_address.into(), false),
            AccountMeta::new_readonly(target_epoch_address.into(), false),
            AccountMeta::new_readonly(target_committee_address.into(), false),
            AccountMeta::new_readonly(peer_set_address.into(), false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}
