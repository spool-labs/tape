use steel::*;
use crankx::Solution;

use crate::{
    consts::*, 
    instruction::*, 
    pda::*,
    utils,
};

pub fn build_create_ix(
    signer: Pubkey,
    name: &str,
    header: Option<[u8; HEADER_SIZE]>,
) -> Instruction {
    let name = utils::to_name(name);
    let header = header.unwrap_or([0; HEADER_SIZE]);

    let (tape_address, _tape_bump) = tape_pda(signer, &name);
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(writer_address, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Create {
            name,
            header,
        }.to_bytes(),
    }
}

pub fn build_write_ix(
    signer: Pubkey,
    tape: Pubkey,
    writer: Pubkey,
    data: &[u8],
) -> Instruction {

    let mut ix_data = Write{}.to_bytes();
    ix_data.extend_from_slice(data);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
        ],
        data: ix_data,
    }
}

pub fn build_update_ix(
    signer: Pubkey,
    tape: Pubkey,
    writer: Pubkey,
    segment_number: u64,
    old_data: [u8; SEGMENT_SIZE],
    new_data: [u8; SEGMENT_SIZE],
    proof: [[u8;32]; PROOF_LEN],
) -> Instruction {

    let segment_number = segment_number.to_le_bytes();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
        ],
        data: Update {
            segment_number,
            old_data,
            new_data,
            proof,
        }.to_bytes(),
    }
}

pub fn build_finalize_ix(
    signer: Pubkey, 
    tape: Pubkey,
    writer: Pubkey,
    header: Option<[u8; HEADER_SIZE]>,
) -> Instruction {
    let header = header.unwrap_or([0; HEADER_SIZE]);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
            AccountMeta::new(ARCHIVE_ADDRESS, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Finalize {
            header,
        }.to_bytes(),
    }
}

pub fn build_register_ix(
    signer: Pubkey, 
    name: &str
) -> Instruction {
    let name = utils::to_name(name);
    let (miner_address, _bump) = miner_pda(signer, name);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Register {
            name,
        }.to_bytes(),
    }
}

pub fn build_mine_ix(
    signer: Pubkey,
    miner: Pubkey,
    tape: Pubkey,
    solution: Solution,
    recall_segment: [u8; SEGMENT_SIZE],
    recall_proof: [[u8;32]; PROOF_LEN],
) -> Instruction {

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(EPOCH_ADDRESS, false),
            AccountMeta::new(BLOCK_ADDRESS, false),
            AccountMeta::new(miner, false),
            AccountMeta::new_readonly(tape, false),
            AccountMeta::new_readonly(ARCHIVE_ADDRESS, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Mine {
            digest: solution.d,
            nonce: solution.n,
            recall_segment,
            recall_proof,
        }.to_bytes(),
    }
}

pub fn build_claim_ix(
    signer: Pubkey, 
    miner: Pubkey,
    beneficiary: Pubkey, 
    amount: u64
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(beneficiary, false),
            AccountMeta::new(miner, false),
            AccountMeta::new_readonly(TREASURY_ADDRESS, false),
            AccountMeta::new(TREASURY_ATA, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: Claim {
            amount: amount.to_le_bytes(),
        }.to_bytes(),
    }
}

pub fn build_close_ix(
    signer: Pubkey,
    miner: Pubkey,
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: Close {}.to_bytes(),
    }
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
