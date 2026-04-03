use tape_solana::*;
use tape_crypto::address::Address;
use crate::program::tapedrive;
use crate::program::tapedrive::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}

pub fn build_advance_epoch_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(epoch_address.into(), false),
            AccountMeta::new_readonly(snapshot_state_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}
