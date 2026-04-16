use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};

use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::{BlobInfo, PackedBlobInfo};
use tape_core::types::{ChunkNumber, EpochNumber, SpoolGroupBitmap};
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::{
    epoch_pda, snapshot_pda, snapshot_tape_pda, system_pda,
};

/// Permissionless instruction to create the snapshot manifest and tape accounts for each epoch.
/// This can be called by anyone, but must be called before any snapshot signing can occur for the
/// epoch.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ReserveSnapshot {}


/// Write a quorum-signed snapshot chunk to the manifest. This instruction is sent by the
/// SpoolGroup members after they have collected enough signatures for a given snapshot chunk.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WriteSnapshot {
    pub group: [u8; 8],                   // The SpoolGroup packed as bytes
    pub chunk: [u8; 8],             // Snapshot chunk index
    pub bitmap: SpoolGroupBitmap,         // A bitmap indicating which SpoolGroup members have signed
    pub signature: BlsSignature,          // The aggregated BLS signature from the committee members
    pub snapshot: PackedBlobInfo,         // The BlobInfo for the snapshot chunk being signed
}

unsafe impl Pod for WriteSnapshot {}
unsafe impl Zeroable for WriteSnapshot {}

/// A quorum-signed instruction to finalize a groups contribution to the snapshot manifest. This
/// instruction is sent by SpoolGroup members after they have written all of their snapshot chunks
/// to the manifest.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SignSnapshot {
    pub group: [u8; 8],
    pub bitmap: SpoolGroupBitmap,
    pub signature: BlsSignature,
}

unsafe impl Pod for SignSnapshot {}
unsafe impl Zeroable for SignSnapshot {}

pub fn build_reserve_snapshot_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (epoch_address, _) = epoch_pda();
    let (snapshot_address, _) = snapshot_pda(epoch);
    let (tape_address, _) = snapshot_tape_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(snapshot_address.into(), false),
            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ReserveSnapshot {}
        .to_bytes(),
    }
}

pub fn build_write_snapshot_ix(
    fee_payer: Address,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    bitmap: SpoolGroupBitmap,
    signature: BlsSignature,
    blob: &BlobInfo,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (snapshot_address, _) = snapshot_pda(epoch);
    let (tape_address, _) = snapshot_tape_pda(epoch);

    let chunk = chunk.pack();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new_readonly(snapshot_address.into(), false),
            AccountMeta::new(tape_address.into(), false),
        ],
        data: WriteSnapshot {
            group: group.pack(),
            chunk,
            bitmap,
            signature,
            snapshot: blob.pack(),
        }
        .to_bytes(),
    }
}

pub fn build_sign_snapshot_ix(
    fee_payer: Address,
    epoch: EpochNumber,
    group: SpoolGroup,
    bitmap: SpoolGroupBitmap,
    signature: BlsSignature,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (snapshot_address, _) = snapshot_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(snapshot_address.into(), false),
        ],
        data: SignSnapshot {
            group: group.pack(),
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}
