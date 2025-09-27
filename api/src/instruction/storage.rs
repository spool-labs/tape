use steel::*;
use crate::pda::*;
use tape_core::prelude::*;
use spl_associated_token_account::get_associated_token_address;

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
    let (treasury_address, _) = treasury_pda();
    let (treasury_ata, _) = treasury_ata();

    let (mint_address, _) = mint_pda();
    let (resource_address, _) = resource_pda(signer);
    let signer_ata = get_associated_token_address(&signer, &mint_address);

    let storage_units = storage_units.pack();
    let start_epoch = start_epoch.pack();
    let end_epoch = end_epoch.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(resource_address, false),

            AccountMeta::new(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(treasury_address, false),
            AccountMeta::new(treasury_ata, false),

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
