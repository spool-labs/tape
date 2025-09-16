use steel::*;
use crate::{
    consts::*,
    pda::*,
    utils,
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum ProgramInstruction {
    Unknown = 0,
    Initialize, // Initialize the program, setting up necessary accounts
    Airdrop,    // Airdrop tokens to the fee payer (devnet/localnet only)
}

instruction!(ProgramInstruction, Initialize);
instruction!(ProgramInstruction, Airdrop);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Airdrop {
    pub amount: [u8; 8],
}

pub fn build_initialize_ix(
    signer: Pubkey
) -> Instruction {

    let (archive_pda, _archive_bump) = archive_pda();
    let (epoch_pda, _epoch_bump) = epoch_pda();
    let (block_pda, _block_bump) = block_pda();
    let (mint_pda, _mint_bump) = mint_pda();
    let (treasury_pda, _treasury_bump) = treasury_pda();
    let (treasury_ata, _treasury_ata_bump) = treasury_ata();
    let (metadata_pda, _metadata_bump) = metadata_pda(mint_pda);

    let name = utils::to_name("genesis");
    let (tape_pda, _tape_bump) = tape_pda(signer, &name);
    let (writer_pda, _writer_bump) = writer_pda(tape_pda);

    assert_eq!(archive_pda, ARCHIVE_ADDRESS);
    assert_eq!(epoch_pda, EPOCH_ADDRESS);
    assert_eq!(block_pda, BLOCK_ADDRESS);
    assert_eq!(mint_pda, MINT_ADDRESS);
    assert_eq!(treasury_pda, TREASURY_ADDRESS);
    assert_eq!(treasury_ata, TREASURY_ATA);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(archive_pda, false),
            AccountMeta::new(epoch_pda, false),
            AccountMeta::new(block_pda, false),
            AccountMeta::new(metadata_pda, false),
            AccountMeta::new(mint_pda, false),
            AccountMeta::new(treasury_pda, false),
            AccountMeta::new(treasury_ata, false),
            AccountMeta::new(tape_pda, false),
            AccountMeta::new(writer_pda, false),
            AccountMeta::new_readonly(crate::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Initialize {}.to_bytes(),
    }
}

pub fn build_airdrop_ix(
    signer: Pubkey,
    beneficiary: Pubkey, 
    amount: u64
) -> Instruction {
    let (mint_pda, _mint_bump) = mint_pda();
    let (treasury_pda, _treasury_bump) = treasury_pda();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(beneficiary, false),
            AccountMeta::new(mint_pda, false),
            AccountMeta::new(treasury_pda, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: Airdrop {
            amount: amount.to_le_bytes(),
        }.to_bytes(),
    }
}
