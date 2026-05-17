use core::mem::size_of;

use crate::helpers::read_instruction_pod;
use crate::program::tapedrive;
use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackDataSlice;
use tape_core::track::types::{CompressedTrackProof, TrackKind};
use tape_core::types::{EpochNumber, SpoolBitmap, StorageUnits, StripeCount};
use tape_crypto::Hash;
use tape_solana::*;
use tape_crypto::address::Address;
use crate::program::tapedrive::{group_pda, system_pda, tape_pda};

pub const TRACK_WRITE_MAX_BYTES: usize = 10 * 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackWrite {
    pub key: Hash,
    pub kind: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {
    pub track: CompressedTrackProof,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyTrack {
    pub track: CompressedTrackProof,
    pub epoch: [u8; 8],
    pub bitmap: SpoolBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateTrack {
    pub track: CompressedTrackProof,
    pub bitmap: SpoolBitmap,
    pub signature: BlsSignature,
    pub computed_root: Hash,
}


pub fn build_track_write_blob_ix(
    fee_payer: Address,
    authority: Address,
    key: Hash,          // Track identifier (e.g., file path hash)
    blob: BlobInfo,
) -> Result<Instruction, ProgramError> {
    if blob.stripe_size == StorageUnits::zero() {
        return Err(ProgramError::InvalidInstructionData);
    }

    if blob.stripe_count == StripeCount::zero() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (system_address, _) = system_pda();
    let (tape_address, _) = tape_pda(authority);

    Ok(Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: make_blob(key, blob),
    })
}

pub fn build_track_write_raw_ix(
    fee_payer: Address,
    authority: Address,
    key: Hash,
    raw: &[u8],
) -> Result<Instruction, ProgramError> {
    let (system_address, _) = system_pda();
    let (tape_address, _) = tape_pda(authority);

    Ok(Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: make_raw(key, raw)?,
    })
}

pub fn build_delete_track_ix(
    fee_payer: Address,
    authority: Address,
    track: CompressedTrackProof,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(track.state.tape.into(), false),
        ],
        data: DeleteTrack { track }.to_bytes(),
    }
}

pub fn build_certify_track_ix(
    fee_payer: Address,
    authority: Address,
    track: CompressedTrackProof,
    epoch: EpochNumber,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (group_address, _) = group_pda(epoch, track.state.spool_group);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(group_address.into(), false),
            AccountMeta::new(track.state.tape.into(), false),
        ],
        data: CertifyTrack {
            track,
            epoch: epoch.pack(),
            bitmap,
            signature,
        }.to_bytes(),
    }
}

pub fn build_invalidate_track_ix(
    fee_payer: Address,
    track: CompressedTrackProof,
    epoch: EpochNumber,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
    computed_root: Hash,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (group_address, _) = group_pda(epoch, track.state.spool_group);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(group_address.into(), false),
            AccountMeta::new(track.state.tape.into(), false),
        ],
        data: InvalidateTrack {
            track,
            bitmap,
            signature,
            computed_root,
        }.to_bytes(),
    }
}

#[inline(always)]
fn split_track_write_data(data: &[u8]) -> Result<(TrackWrite, &[u8]), ProgramError> {
    if data.len() < size_of::<TrackWrite>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (header, value) = data.split_at(size_of::<TrackWrite>());
    let header = read_instruction_pod::<TrackWrite>(header)?;

    Ok((header, value))
}

#[inline(always)]
pub fn parse_track_write(data: &[u8]) -> Result<(TrackWrite, TrackDataSlice<'_>), ProgramError> {
    let (header, value) = split_track_write_data(data)?;

    let kind = TrackKind::try_from(u64::from_le_bytes(header.kind))
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let value = match kind {
        TrackKind::Raw => {
            if value.len() > TRACK_WRITE_MAX_BYTES {
                return Err(ProgramError::InvalidInstructionData);
            }

            TrackDataSlice::Raw(value)
        }
        TrackKind::Blob => {
            if value.len() != size_of::<BlobInfo>() {
                return Err(ProgramError::InvalidInstructionData);
            }

            let blob = read_instruction_pod::<BlobInfo>(value)?;

            TrackDataSlice::Blob(blob)
        }
    };

    Ok((header, value))
}

#[inline(always)]
pub fn parse_delete_track(data: &[u8]) -> Result<DeleteTrack, ProgramError> {
    read_instruction_pod::<DeleteTrack>(data)
}

#[inline(always)]
pub fn parse_certify_track(data: &[u8]) -> Result<CertifyTrack, ProgramError> {
    read_instruction_pod::<CertifyTrack>(data)
}

#[inline(always)]
pub fn parse_invalidate_track(data: &[u8]) -> Result<InvalidateTrack, ProgramError> {
    read_instruction_pod::<InvalidateTrack>(data)
}

#[inline(always)]
fn make_raw(key: Hash, raw: &[u8]) -> Result<Vec<u8>, ProgramError> {
    if raw.len() > TRACK_WRITE_MAX_BYTES {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut data = TrackWrite {
        key,
        kind: (TrackKind::Raw as u64).to_le_bytes(),
    }
    .to_bytes();

    data.extend_from_slice(raw);
    Ok(data)
}

#[inline(always)]
fn make_blob(key: Hash, blob: BlobInfo) -> Vec<u8> {
    let mut data = TrackWrite {
        key,
        kind: (TrackKind::Blob as u64).to_le_bytes(),
    }
    .to_bytes();

    data.extend_from_slice(bytemuck::bytes_of(&blob));
    data
}
