use steel::*;
use crate::program::tapedrive::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateEpoch {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvanceEpoch {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SyncEpoch {
    pub epoch: [u8; 8],
    pub seats: Hash,
}

pub fn build_create_epoch_ix(
    signer: Pubkey,
) -> Instruction {
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateEpoch {}.to_bytes(),
    }
}

pub fn build_advance_epoch_ix(
    signer: Pubkey
) ->Instruction {
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(epoch_address, false),
        ],
        data: AdvanceEpoch {}.to_bytes(),
    }
}

pub fn build_epoch_sync_ix(
    signer: Pubkey,
    epoch: EpochNumber,
    seats: &[SeatIndex],
) ->Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (node_address, _) = node_pda(signer);

    let epoch = epoch.pack();
    let seats = get_seat_hash(seats);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),
        ],
        data: SyncEpoch {
            epoch,
            seats,
        }.to_bytes(),
    }
}
