use steel::*;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::tapedrive::*;

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
    signer: Pubkey,
    storage_units: StorageUnits,
    activation_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Instruction {

    let signer_ata = ata(&signer);
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();

    let (tape_address, _) = tape_pda(signer);

    let storage_units = storage_units.pack();
    let activation_epoch = activation_epoch.pack();
    let expiry_epoch = expiry_epoch.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(tape_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(archive_ata, false),

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
    signer: Pubkey,
    recipient: Pubkey,
    size: StorageUnits,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(signer);
    let (dest_tape_address, _) = tape_pda(recipient);
    let (archive_address, _) = archive_pda();

    let size = size.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(recipient, true),

            AccountMeta::new(source_tape_address, false),
            AccountMeta::new(dest_tape_address, false),
            AccountMeta::new(archive_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: SplitTapeBySize { size }.to_bytes(),
    }
}

pub fn build_split_tape_by_epoch_ix(
    signer: Pubkey,
    recipient: Pubkey,
    split_epoch: EpochNumber,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(signer);
    let (dest_tape_address, _) = tape_pda(recipient);
    let (archive_address, _) = archive_pda();

    let epoch = split_epoch.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(recipient, true),

            AccountMeta::new(source_tape_address, false),
            AccountMeta::new(dest_tape_address, false),
            AccountMeta::new(archive_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: SplitTapeByEpoch { epoch }.to_bytes(),
    }
}

pub fn build_merge_tape_ix(
    signer: Pubkey,
    recipient: Pubkey,
) -> Instruction {
    let (source_tape_address, _) = tape_pda(signer);
    let (dest_tape_address, _) = tape_pda(recipient);
    let (archive_address, _) = archive_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(recipient, true),

            AccountMeta::new(source_tape_address, false),
            AccountMeta::new(dest_tape_address, false),
            AccountMeta::new(archive_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: MergeTape {}.to_bytes(),
    }
}

pub fn build_destroy_tape_ix(
    signer: Pubkey,
) -> Instruction {
    let (tape_address, _) = tape_pda(signer);
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new(tape_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(archive_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DestroyTape {}.to_bytes(),
    }
}
