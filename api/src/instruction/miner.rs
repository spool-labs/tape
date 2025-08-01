use steel::*;
use crate::{
    consts::*,
    pda::*,
    types::*,
    utils,
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum MinerInstruction {
    Register = 0x20, // Register a miner (pubkey, name) pair
    Unregister,      // Unregister a miner account, returning the balance to the miner
    Mine,            // Mine a block, providing proof of storage
    Claim,           // Claim earned mining rewards
}

instruction!(MinerInstruction, Register);
instruction!(MinerInstruction, Unregister);
instruction!(MinerInstruction, Mine);
instruction!(MinerInstruction, Claim);


#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Register {
    pub name: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unregister {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Mine {
    pub pow: PoW,
    pub poa: PoA,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Claim {
    pub amount: [u8; 8],
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
    pow: PoW,
    poa: PoA,
) -> Instruction {

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(EPOCH_ADDRESS, false),
            AccountMeta::new(BLOCK_ADDRESS, false),
            AccountMeta::new(miner, false),
            AccountMeta::new(tape, false),
            AccountMeta::new_readonly(ARCHIVE_ADDRESS, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Mine {
            pow,
            poa,
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
        data: Unregister {}.to_bytes(),
    }
}
