use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use crate::program::token::mint_pda;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ReserveTape {
    pub storage_units: StorageUnits,
    pub activation_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DestroyTape {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeByEpoch {
    pub epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeBySize {
    pub size: StorageUnits,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergeTape {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetTapeDelegate {
    pub delegate: Address,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RevokeTapeDelegate {}

pub fn build_reserve_tape_ix(
    fee_payer: Address,
    authority: Address,
    storage_units: StorageUnits,
    activation_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Instruction {

    let authority_ata = ata(&authority);
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();
    let (mint_address, _) = mint_pda();

    let (tape_address, _) = tape_pda(authority);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),
            AccountMeta::new(mint_address.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ReserveTape {
            storage_units,
            activation_epoch,
            expiry_epoch,
        }.to_bytes(),
    }
}

pub fn build_split_tape_by_size_ix(
    fee_payer: Address,
    authority: Address,
    recipient: Address,
    size: StorageUnits,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(authority);
    let (dest_tape_address, _) = tape_pda(recipient);
    let (archive_address, _) = archive_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(recipient.into(), true),

            AccountMeta::new(source_tape_address.into(), false),
            AccountMeta::new(dest_tape_address.into(), false),
            AccountMeta::new(archive_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: SplitTapeBySize { size }.to_bytes(),
    }
}

pub fn build_split_tape_by_epoch_ix(
    fee_payer: Address,
    authority: Address,
    recipient: Address,
    split_epoch: EpochNumber,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(authority);
    let (dest_tape_address, _) = tape_pda(recipient);
    let (archive_address, _) = archive_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(recipient.into(), true),

            AccountMeta::new(source_tape_address.into(), false),
            AccountMeta::new(dest_tape_address.into(), false),
            AccountMeta::new(archive_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: SplitTapeByEpoch { epoch: split_epoch }.to_bytes(),
    }
}

pub fn build_merge_tape_ix(
    fee_payer: Address,
    authority: Address,
    recipient: Address,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(authority);
    let (dest_tape_address, _) = tape_pda(recipient);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(recipient.into(), true),

            AccountMeta::new(source_tape_address.into(), false),
            AccountMeta::new(dest_tape_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: MergeTape {}.to_bytes(),
    }
}

pub fn build_destroy_tape_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {
    let (tape_address, _) = tape_pda(authority);
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DestroyTape {}.to_bytes(),
    }
}

pub fn build_set_tape_delegate_ix(
    fee_payer: Address,
    authority: Address,
    tape: Address,
    delegate: Address,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(tape.into(), false),
        ],
        data: SetTapeDelegate { delegate }.to_bytes(),
    }
}

pub fn build_revoke_tape_delegate_ix(
    fee_payer: Address,
    authority: Address,
    tape: Address,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(tape.into(), false),
        ],
        data: RevokeTapeDelegate {}.to_bytes(),
    }
}
