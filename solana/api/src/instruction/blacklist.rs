use tape_core::system::BlacklistEntry;
use tape_core::track::types::CompressedTrackProof;
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist {
    pub entry: BlacklistEntry,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {
    pub track: CompressedTrackProof,
}

pub fn build_add_to_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
    entry: BlacklistEntry,
) -> Instruction {
    let (blacklist_address, _) = blacklist_pda(node);
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: AddToBlacklist { entry }.to_bytes(),
    }
}

pub fn build_remove_from_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
    track: CompressedTrackProof,
) -> Instruction {
    let (blacklist_address, _) = blacklist_pda(node);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
        ],
        data: RemoveFromBlacklist { track }.to_bytes(),
    }
}
