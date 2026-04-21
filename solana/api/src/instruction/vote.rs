use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};

use tape_crypto::address::Address;

use crate::program::tapedrive;
use crate::program::tapedrive::epoch_pda;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CloseVote {}

pub fn build_close_vote_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    vote: Address,
) -> Instruction {
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),
            AccountMeta::new_readonly(node_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(vote.into(), false),
        ],
        data: CloseVote {}.to_bytes(),
    }
}
