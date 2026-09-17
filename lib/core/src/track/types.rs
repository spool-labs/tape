//! Track protocol types and merkle proofs.

use core::mem::size_of;
use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "wincode")]
use core::mem::MaybeUninit;
#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

use tape_crypto::address::Address;
use tape_crypto::hash::{hashv, Hash};
use tape_crypto::merkle::{MerkleError, MerkleTree};

use crate::spooler::GroupIndex;
use crate::types::{StorageUnits, TrackNumber};
use super::archive::TRACK_TREE_HEIGHT;

pub const TRACK_LEAF_V1: &[u8; 8] = b"TRACK_V1";

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TrackKind {
    Inline = 0,
    Coded,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TrackState {
    Registered = 0,
    Certified,
    Invalidated,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TrackVisibility {
    Public = 0,
    Hidden,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize))]
pub struct CompressedTrack {
    pub tape: Address,
    pub track_number: TrackNumber,
    pub key: Hash,
    pub kind: u64,
    pub state: u64,
    pub visibility: u64,
    pub size: StorageUnits,
    pub group: GroupIndex,
    pub value_hash: Hash,
}

impl CompressedTrack {
    #[inline(always)]
    pub fn kind_enum(&self) -> Option<TrackKind> {
        TrackKind::try_from(self.kind).ok()
    }

    #[inline(always)]
    pub fn state_enum(&self) -> Option<TrackState> {
        TrackState::try_from(self.state).ok()
    }

    #[inline(always)]
    pub fn visibility_enum(&self) -> Option<TrackVisibility> {
        TrackVisibility::try_from(self.visibility).ok()
    }

    #[inline(always)]
    pub fn is_inline(&self) -> bool {
        matches!(self.kind_enum(), Some(TrackKind::Inline))
    }

    #[inline(always)]
    pub fn is_coded(&self) -> bool {
        matches!(self.kind_enum(), Some(TrackKind::Coded))
    }

    #[inline(always)]
    pub fn is_registered(&self) -> bool {
        matches!(self.state_enum(), Some(TrackState::Registered))
    }

    #[inline(always)]
    pub fn is_certified(&self) -> bool {
        matches!(self.state_enum(), Some(TrackState::Certified))
    }

    #[inline(always)]
    pub fn is_invalidated(&self) -> bool {
        matches!(self.state_enum(), Some(TrackState::Invalidated))
    }

    /// Anything this binary cannot read as Public is treated as hidden, so an
    /// unrecognized visibility withholds bytes.
    #[inline(always)]
    pub fn is_hidden(&self) -> bool {
        !matches!(self.visibility_enum(), Some(TrackVisibility::Public))
    }

    #[inline(always)]
    pub fn get_hash(&self) -> Hash {
        hashv(&[
            TRACK_LEAF_V1,
            &self.tape.to_bytes(),
            &self.track_number.pack(),
            &self.key.to_bytes(),
            &self.kind.to_le_bytes(),
            &self.state.to_le_bytes(),
            &self.visibility.to_le_bytes(),
            &self.size.pack(),
            &self.group.pack(),
            &self.value_hash.to_bytes(),
        ])
    }

    #[inline(always)]
    pub fn pack(&self) -> PackedTrack {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    #[inline(always)]
    pub fn unpack(data: PackedTrack) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }
}

#[cfg(feature = "wincode")]
impl SchemaWrite for CompressedTrack {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(size_of::<Self>())
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.pack())?;
        Ok(())
    }
}

#[cfg(feature = "wincode")]
impl<'de> SchemaRead<'de> for CompressedTrack {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // SAFETY: `PackedTrack` is a byte-aligned repr-specified representation for
        // `CompressedTrack`, and `reader.get_t()` only reads exactly that size.
        let packed: PackedTrack = unsafe { reader.get_t()? };
        dst.write(Self::unpack(packed));
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct CompressedTrackProof {
    pub state: CompressedTrack,
    pub proof: [Hash; TRACK_TREE_HEIGHT],
}

pub type PackedTrack = [u8; size_of::<CompressedTrack>()];
pub type PackedTrackProof = [u8; size_of::<CompressedTrackProof>()];

impl CompressedTrackProof {
    #[inline(always)]
    pub fn verify(&self, tree: &MerkleTree<TRACK_TREE_HEIGHT>) -> Result<Hash, MerkleError> {
        let mut tree = *tree;
        tree.ensure_initialized();

        let track_hash = self.state.get_hash();
        if tree.verify_hash(self.state.track_number.0, &self.proof, track_hash)? {
            Ok(track_hash)
        } else {
            Err(MerkleError::InvalidProof)
        }
    }

    #[inline(always)]
    pub fn pack(&self) -> PackedTrackProof {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    #[inline(always)]
    pub fn unpack(data: PackedTrackProof) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public_track() -> CompressedTrack {
        CompressedTrack {
            tape: Address::new_unique(),
            track_number: TrackNumber(7),
            key: Hash::new_unique(),
            kind: TrackKind::Coded as u64,
            state: TrackState::Registered as u64,
            visibility: TrackVisibility::Public as u64,
            size: StorageUnits::from_bytes(4096),
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        }
    }

    // a zeroed leaf reads as a public track
    #[test]
    fn zeroed_is_public() {
        let track = CompressedTrack::zeroed();

        assert_eq!(track.visibility_enum(), Some(TrackVisibility::Public));
        assert!(!track.is_hidden());
    }

    // visibility is committed by the leaf hash
    #[test]
    fn visibility_in_hash() {
        let public = public_track();
        let mut hidden = public;
        hidden.visibility = TrackVisibility::Hidden as u64;

        assert_ne!(public.get_hash(), hidden.get_hash());
        assert!(hidden.is_hidden());
    }

    // packing round-trips every field
    #[test]
    fn pack_roundtrip() {
        let mut track = public_track();
        track.visibility = TrackVisibility::Hidden as u64;

        let restored = CompressedTrack::unpack(track.pack());

        assert_eq!(track, restored);
    }
}
