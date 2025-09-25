use steel::*;
use crate::pda::*;
use spl_associated_token_account::get_associated_token_address;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterFeature {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyFeature {}


pub fn build_initialize_ix(
    signer: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (treasury_address, _) = treasury_pda();
    let (treasury_ata, _) = treasury_ata();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();
    let (metadata_address, _) = metadata_pda();

    let signer_ata = get_associated_token_address(&signer, &mint_address);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(treasury_address, false),
            AccountMeta::new(treasury_ata, false),
            AccountMeta::new(mint_address, false),
            AccountMeta::new(metadata_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Initialize {}.to_bytes(),
    }
}

pub fn build_advance_epoch_ix(
    signer: Pubkey
) ->Instruction {
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(epoch_address, false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}
