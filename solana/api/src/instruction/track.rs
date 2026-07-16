use core::mem::size_of;

use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::{
    BlobData, BlobDataSlice, BlobInfo, BlobInfoSlice, TrackObjectInfoSlice,
};
use tape_core::track::types::{CompressedTrackProof, TrackKind};
use tape_core::types::{ContentType, EpochNumber, SpoolBitmap, StorageUnits, StripeCount};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_solana::*;

use crate::helpers::read_instruction_pod;
use crate::program::tapedrive::{self, group_pda, system_pda};

pub const TRACK_WRITE_MAX_BYTES: usize = 10 * 1024;
pub const MAX_NAME_LEN: usize = 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct TrackWrite {
    pub kind: u8,
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
    signer: Address,
    tape: Address,
    blob: BlobInfo,
) -> Result<Instruction, ProgramError> {
    let (system_address, _) = system_pda();

    Ok(Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(signer.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(tape.into(), false),
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
    let (header, payload, trailer) = split_write(data)?;
    let data = parse_payload(header.kind, payload)?;
    let object = parse_object(trailer)?;

    Ok((header, BlobInfoSlice { object, data }))
}

fn split_write(data: &[u8]) -> Result<(TrackWrite, &[u8], &[u8]), ProgramError> {
    if data.len() < size_of::<TrackWrite>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (header, rest) = data.split_at(size_of::<TrackWrite>());
    let header = read_instruction_pod::<TrackWrite>(header)?;

    let data_len = u16::from_le_bytes(header.data_len) as usize;
    if rest.len() < data_len {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (payload, trailer) = rest.split_at(data_len);

    Ok((header, payload, trailer))
}

fn parse_payload(kind: u8, payload: &[u8]) -> Result<BlobDataSlice<'_>, ProgramError> {
    let kind =
        TrackKind::try_from(kind as u64).map_err(|_| ProgramError::InvalidInstructionData)?;

    match kind {
        TrackKind::Inline => parse_inline(payload),
        TrackKind::Coded => parse_coded(payload),
    }
}

fn parse_inline(payload: &[u8]) -> Result<BlobDataSlice<'_>, ProgramError> {
    if payload.len() > TRACK_WRITE_MAX_BYTES {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(BlobDataSlice::Inline(payload))
}

fn parse_coded(payload: &[u8]) -> Result<BlobDataSlice<'_>, ProgramError> {
    let blob = read_instruction_pod::<BlobEncoding>(payload)?;

    Ok(BlobDataSlice::Coded(blob))
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
struct TrackWriteObject {
    pub name_len: [u8; 2],
    pub content_type: [u8; 2],
    pub logical_size: [u8; 8],
}

pub fn track_write_ix_len(
    payload_len: usize,
    object_name_len: Option<usize>,
) -> Option<usize> {
    let mut len = 1usize
        .checked_add(size_of::<TrackWrite>())?
        .checked_add(payload_len)?;

    if let Some(object_name_len) = object_name_len {
        len = len
            .checked_add(size_of::<TrackWriteObject>())?
            .checked_add(object_name_len)?;
    }

    Some(len)
}

fn parse_object(trailer: &[u8]) -> Result<Option<TrackObjectInfoSlice<'_>>, ProgramError> {
    if trailer.is_empty() {
        return Ok(None);
    }

    if trailer.len() < size_of::<TrackWriteObject>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (object_header, object_name) = trailer.split_at(size_of::<TrackWriteObject>());
    let object_header = read_instruction_pod::<TrackWriteObject>(object_header)?;
    let object_name_len = u16::from_le_bytes(object_header.name_len) as usize;

    check_name(object_name, object_name_len)?;

    let content_type = ContentType::try_from(u16::from_le_bytes(object_header.content_type))
        .map_err(|_| ProgramError::InvalidInstructionData)?;
    let logical_size = StorageUnits::from_bytes(u64::from_le_bytes(object_header.logical_size));

    Ok(Some(TrackObjectInfoSlice {
        name: object_name,
        content_type,
        logical_size,
    }))
}

fn check_name(name: &[u8], expected_len: usize) -> Result<(), ProgramError> {
    if name.len() != expected_len || expected_len == 0 || expected_len > MAX_NAME_LEN {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

#[inline(always)]
fn make_track_write(blob: BlobInfo) -> Result<Vec<u8>, ProgramError> {
    if blob
        .object
        .as_ref()
        .is_some_and(|object| object.name.is_empty() || object.name.len() > MAX_NAME_LEN)
    {
        return Err(ProgramError::InvalidInstructionData);
    }

    let BlobInfo { object, data } = blob;
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
        data_len: data_len.to_le_bytes(),
    }
    .to_bytes();

    out.extend_from_slice(&payload);

    if let Some(object) = object {
        let object_name_len = u16::try_from(object.name.len())
            .map_err(|_| ProgramError::InvalidInstructionData)?;

        out.extend_from_slice(bytemuck::bytes_of(&TrackWriteObject {
            name_len: object_name_len.to_le_bytes(),
            content_type: u16::from(object.content_type).to_le_bytes(),
            logical_size: object.logical_size.to_bytes().to_le_bytes(),
        }));

        out.extend_from_slice(&object.name);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::track::data::TrackObjectInfo;
    use tape_crypto::merkle::root_from_leaf_hashes;

    fn valid_blob(size: StorageUnits) -> BlobEncoding {
        let leaves = [Hash::from([0x11; 32]); GROUP_SIZE];
        BlobEncoding {
            size,
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(1),
            leaves,
        }
    }

    #[test]
    fn track_write_roundtrips_object_logical_size() {
        let manifest_size = StorageUnits::from_bytes(113);
        let logical_size = StorageUnits::from_bytes(100 * 1024 * 1024);
        let name = b"roms/100_omg.bin".to_vec();
        let blob = valid_blob(manifest_size);

        let ix = build_track_write_ix(
            Address::new_unique(),
            Address::new_unique(),
            Address::new_unique(),
            BlobInfo {
                object: Some(TrackObjectInfo {
                    name: name.clone(),
                    content_type: ContentType::ImageJpeg,
                    logical_size,
                }),
                data: BlobData::Coded(blob),
            },
        )
        .expect("track write instruction");
        assert_eq!(
            track_write_ix_len(bytemuck::bytes_of(&blob).len(), Some(name.len())),
            Some(ix.data.len())
        );

        let (_header, parsed) = parse_track_write(&ix.data[1..]).expect("parse track write");
        let object = parsed.object.expect("object metadata");
        assert_eq!(object.name, name.as_slice());
        assert_eq!(object.content_type, ContentType::ImageJpeg);
        assert_eq!(object.logical_size, logical_size);

        let BlobDataSlice::Coded(parsed_blob) = parsed.data else {
            panic!("expected coded blob metadata");
        };
        assert_eq!(parsed_blob.size, manifest_size);
    }

    #[test]
    fn track_write_without_trailer_has_no_object() {
        let payload = b"chunk-data".to_vec();
        let ix = build_track_write_ix(
            Address::new_unique(),
            Address::new_unique(),
            Address::new_unique(),
            BlobInfo {
                object: None,
                data: BlobData::Inline(payload.clone()),
            },
        )
        .expect("track write instruction");
        assert_eq!(
            track_write_ix_len(payload.len(), None),
            Some(ix.data.len())
        );

        let (_header, parsed) = parse_track_write(&ix.data[1..]).expect("parse track write");
        assert!(parsed.object.is_none());

        let BlobDataSlice::Inline(parsed_payload) = parsed.data else {
            panic!("expected inline payload");
        };
        assert_eq!(parsed_payload, payload.as_slice());
    }

    #[test]
    fn track_write_rejects_partial_object_trailer() {
        let mut ix = build_track_write_ix(
            Address::new_unique(),
            Address::new_unique(),
            Address::new_unique(),
            BlobInfo {
                object: None,
                data: BlobData::Inline(b"chunk-data".to_vec()),
            },
        )
        .expect("track write instruction");
        ix.data.push(0xff);

        let err = parse_track_write(&ix.data[1..]).expect_err("partial object trailer");
        assert_eq!(err, ProgramError::InvalidInstructionData);
    }
}
