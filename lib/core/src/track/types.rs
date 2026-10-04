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

pub const TRACK_LEAF: &[u8; 8] = b"TRACK\0\0\0";
pub const TRACK_CONTENT: &[u8; 8] = b"CONTENT\0";

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

pub struct TrackFlags;

impl TrackFlags {
    pub const NONE: u64 = 0;
    pub const HIDDEN: u64 = 1;

    const KNOWN: u64 = Self::HIDDEN;

    #[inline(always)]
    pub fn is_known(flags: u64) -> bool {
        flags & !Self::KNOWN == 0
    }

    #[inline(always)]
    pub fn is_hidden(flags: u64) -> bool {
        flags & Self::HIDDEN != 0
    }
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
    pub flags: u64,
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

    #[inline(always)]
    pub fn is_hidden(&self) -> bool {
        TrackFlags::is_hidden(self.flags)
    }

    #[inline(always)]
    pub fn get_hash(&self) -> Hash {
        hashv(&[
            TRACK_LEAF,
            &self.content_address().to_bytes(),
            &self.track_number.pack(),
            &self.state.to_le_bytes(),
            &self.group.pack(),
        ])
    }

    #[inline(always)]
    pub fn content_address(&self) -> Hash {
        hashv(&[
            TRACK_CONTENT,
            &self.tape.to_bytes(),
            &self.key.to_bytes(),
            &self.kind.to_le_bytes(),
            &self.flags.to_le_bytes(),
            &self.size.pack(),
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
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(4096),
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        }
    }

    // a zeroed leaf reads as a public track
    #[test]
    fn zeroed_is_public() {
        let track = CompressedTrack::zeroed();

        assert_eq!(track.flags, TrackFlags::NONE);
        assert!(!track.is_hidden());
    }

    // flags are committed by the leaf hash
    #[test]
    fn flags_in_hash() {
        let public = public_track();
        let mut hidden = public;
        hidden.flags = TrackFlags::HIDDEN;

        assert_ne!(public.get_hash(), hidden.get_hash());
        assert!(hidden.is_hidden());
    }

    // a certify signs the leaf hash, so two tracks with the same content differ in it
    #[test]
    fn number_in_hash() {
        let track = public_track();
        let rewritten = CompressedTrack { track_number: TrackNumber(4), ..track };

        assert_eq!(track.content_address(), rewritten.content_address());
        assert_ne!(track.get_hash(), rewritten.get_hash());
    }

    // only the hidden flag hides a track
    #[test]
    fn unknown_flag_stays_public() {
        let mut track = public_track();
        track.flags = 1 << 40;

        assert!(!TrackFlags::is_known(track.flags));
        assert!(!track.is_hidden());
    }

    // where a track sits and how far it got leave its content address alone
    #[test]
    fn content_address_ignores_position() {
        let track = public_track();

        let certified = CompressedTrack { state: TrackState::Certified as u64, ..track };
        let moved = CompressedTrack { group: GroupIndex(9), ..track };
        let rewritten = CompressedTrack { track_number: TrackNumber(4), ..track };

        assert_eq!(track.content_address(), certified.content_address());
        assert_eq!(track.content_address(), moved.content_address());
        assert_eq!(track.content_address(), rewritten.content_address());
    }

    // every field that says what a track holds moves its content address
    #[test]
    fn content_address_covers_content() {
        let track = public_track();

        let changed = [
            CompressedTrack { tape: Address::new_unique(), ..track },
            CompressedTrack { key: Hash::new_unique(), ..track },
            CompressedTrack { kind: TrackKind::Inline as u64, ..track },
            CompressedTrack { flags: TrackFlags::HIDDEN, ..track },
            CompressedTrack { size: StorageUnits::from_bytes(4097), ..track },
            CompressedTrack { value_hash: Hash::new_unique(), ..track },
        ];

        for other in changed {
            assert_ne!(track.content_address(), other.content_address());
        }
    }

    // packing round-trips every field
    #[test]
    fn pack_roundtrip() {
        let mut track = public_track();
        track.flags = TrackFlags::HIDDEN;

        let restored = CompressedTrack::unpack(track.pack());

        assert_eq!(track, restored);
    }
}
