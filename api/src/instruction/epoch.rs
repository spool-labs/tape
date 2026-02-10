use tape_solana::*;
use crate::program::tapedrive::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}

pub fn build_advance_epoch_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}

