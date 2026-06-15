use core::mem::size_of;

use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::{BlobData, BlobDataSlice, BlobInfo, BlobInfoSlice, ContentHint};
use tape_core::track::types::{CompressedTrackProof, TrackKind};
use tape_core::types::{EpochNumber, SpoolBitmap, StorageUnits, StripeCount};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_solana::*;

use crate::helpers::read_instruction_pod;
use crate::program::tapedrive::{self, group_pda, system_pda, tape_pda};

pub const TRACK_WRITE_MAX_BYTES: usize = 10 * 1024;
pub const MAX_NAME_LEN: usize = 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackWrite {
    pub kind: u8,
    pub hint: [u8; 2],
    pub data_len: [u8; 2],
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

pub fn build_track_write_ix(
    fee_payer: Address,
    authority: Address,
    blob: BlobInfo,
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
        data: make_track_write(blob)?,
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
    let (group_address, _) = group_pda(epoch, track.state.group);

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
    let (group_address, _) = group_pda(epoch, track.state.group);

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

pub fn parse_track_write(data: &[u8]) -> Result<(TrackWrite, BlobInfoSlice<'_>), ProgramError> {
    if data.len() < size_of::<TrackWrite>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (header, rest) = data.split_at(size_of::<TrackWrite>());
    let header = read_instruction_pod::<TrackWrite>(header)?;

    let data_len = u16::from_le_bytes(header.data_len) as usize;
    if rest.len() < data_len {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (payload, name) = rest.split_at(data_len);
    if name.len() > MAX_NAME_LEN {
        return Err(ProgramError::InvalidInstructionData);
    }

    let kind = TrackKind::try_from(header.kind as u64)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let data = match kind {
        TrackKind::Inline => {
            if payload.len() > TRACK_WRITE_MAX_BYTES {
                return Err(ProgramError::InvalidInstructionData);
            }

            BlobDataSlice::Inline(payload)
        }
        TrackKind::Coded => {
            let blob = read_instruction_pod::<BlobEncoding>(payload)?;

            BlobDataSlice::Coded(blob)
        }
    };

    let hint = ContentHint::try_from(u16::from_le_bytes(header.hint))
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    Ok((header, BlobInfoSlice { name, hint, data }))
}

#[inline(always)]
fn make_track_write(blob: BlobInfo) -> Result<Vec<u8>, ProgramError> {
    if blob.name.len() > MAX_NAME_LEN {
        return Err(ProgramError::InvalidInstructionData);
    }

    let BlobInfo { name, hint, data } = blob;
    let (kind, payload) = match data {
        BlobData::Inline(bytes) => {
            if bytes.len() > TRACK_WRITE_MAX_BYTES {
                return Err(ProgramError::InvalidInstructionData);
            }

            (TrackKind::Inline, bytes)
        }
        BlobData::Coded(blob) => {
            if blob.stripe_size == StorageUnits::zero() {
                return Err(ProgramError::InvalidInstructionData);
            }

            if blob.stripe_count == StripeCount::zero() {
                return Err(ProgramError::InvalidInstructionData);
            }

            (TrackKind::Coded, bytemuck::bytes_of(&blob).to_vec())
        }
    };
    let data_len = u16::try_from(payload.len())
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let mut out = TrackWrite {
        kind: kind as u8,
        hint: u16::from(hint).to_le_bytes(),
        data_len: data_len.to_le_bytes(),
    }
    .to_bytes();

    out.extend_from_slice(&payload);
    out.extend_from_slice(&name);
    Ok(out)
}
