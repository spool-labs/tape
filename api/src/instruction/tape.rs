use steel::*;
use crate::pda::*;
use crate::utils::ata;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ReserveTape {
    pub storage_units: [u8; 8],
    pub start_epoch: [u8; 8],
    pub end_epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct BurnTape {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeByDuration {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitTapeBySize {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergeTape {}


pub fn build_reserve_tape_ix(
    signer: Pubkey,
    storage_units: StorageUnits,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();

    let (tape_address, _) = tape_pda(signer);
    let signer_ata = ata(&signer);

    let storage_units = storage_units.pack();
    let start_epoch = start_epoch.pack();
    let end_epoch = end_epoch.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(tape_address, false),

            AccountMeta::new(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(archive_ata, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ReserveTape {
            storage_units,
            start_epoch,
            end_epoch,
        }.to_bytes(),
    }
}
