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
    pub storage_units: [u8; 8],
    pub activation_epoch: [u8; 8],
    pub expiry_epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DestroyTape {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeByEpoch {
    pub epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeBySize {
    pub size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergeTape {}


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

    let storage_units = storage_units.pack();
    let activation_epoch = activation_epoch.pack();
    let expiry_epoch = expiry_epoch.pack();

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

    let size = size.pack();

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

    let epoch = split_epoch.pack();

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
        data: SplitTapeByEpoch { epoch }.to_bytes(),
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
