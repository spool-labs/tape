use steel::*;
use crate::pda::*;
use crate::types::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum ProgramInstruction {
    Unknown = 0,
    Initialize,
    AdvanceEpoch,

    Airdrop,
}

instruction!(ProgramInstruction, Initialize);
instruction!(ProgramInstruction, AdvanceEpoch);
instruction!(ProgramInstruction, Airdrop);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Airdrop {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}


pub fn build_initialize_ix(
    signer: Pubkey
) -> Instruction {

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();
    let (treasury_address, _) = treasury_pda();
    let (treasury_ata, _) = treasury_ata();
    let (metadata_address, _) = metadata_pda(mint_address);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(metadata_address, false),
            AccountMeta::new(mint_address, false),
            AccountMeta::new(treasury_address, false),
            AccountMeta::new(treasury_ata, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Initialize {}.to_bytes(),
    }
}

pub fn build_airdrop_ix(
    signer: Pubkey,
    beneficiary: Pubkey, 
    amount: Coin<TAPE>
) -> Instruction {
    let (mint_address, _) = mint_pda();
    let (treasury_address, _) = treasury_pda();

    let amount = amount.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(beneficiary, false),
            AccountMeta::new(mint_address, false),
            AccountMeta::new(treasury_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: Airdrop {
            amount,
        }.to_bytes(),
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
