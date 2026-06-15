//! Blob payload metadata and commitment encoding.

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::hash::hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_crypto::merkle::hash_leaf;

use crate::encoding::EncodingProfile;
use crate::erasure::{SLICE_TREE_HEIGHT, GROUP_SIZE};
use crate::types::{SpoolIndex, StorageUnits, StripeCount};

#[cfg(feature = "wincode")]
use core::mem::MaybeUninit;
#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Blob payload metadata stored on nodes responsible for the track's spool group.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize))]
pub struct BlobEncoding {
    /// Original unencoded data size in bytes.
    pub size: StorageUnits,

    /// Root of the erasure-coded commitment tree.
    pub commitment: Hash,

    /// Erasure-coding profile used for the blob.
    pub profile: EncodingProfile,

    /// Stripe size in bytes.
    pub stripe_size: StorageUnits,

    /// Number of stripes.
    pub stripe_count: StripeCount,

    /// Per-slice commitment leaves.
    pub leaves: [Hash; GROUP_SIZE],
}

pub type PackedBlobEncoding = [u8; size_of::<BlobEncoding>()];

impl BlobEncoding {
    #[inline(always)]
    pub fn pack(&self) -> PackedBlobEncoding {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    #[inline(always)]
    pub fn unpack(data: PackedBlobEncoding) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }

    /// Recompute the commitment root from stored leaf hashes.
    pub fn commitment_root(&self) -> Hash {
        root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&self.leaves)
    }

    /// Verify a single slice against its stored leaf hash.
    pub fn verify_slice(&self, position: SpoolIndex, data: &[u8]) -> bool {
        let position = position.as_usize();
        if position >= self.leaves.len() {
            return false;
        }

        hash_leaf(data) == self.leaves[position]
    }

    /// Compute the canonical value hash for this blob payload.
    pub fn get_hash(&self) -> Hash {
        hash(bytemuck::bytes_of(self))
    }
}

#[cfg(feature = "wincode")]
impl SchemaWrite for BlobEncoding {
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
impl<'de> SchemaRead<'de> for BlobEncoding {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // SAFETY: The serialized representation is exactly `PackedBlobEncoding` bytes for this
        // pod-compatible type.
        let packed: PackedBlobEncoding = unsafe { reader.get_t()? };
        dst.write(Self::unpack(packed));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;

    fn sample_blob_encoding() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::from_bytes(512),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
            leaves: [Hash::from([0x33; 32]); GROUP_SIZE],
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn encoding_wincode() {
        let blob = sample_blob_encoding();
        let bytes = wincode::serialize(&blob).expect("serialize");
        let recovered: BlobEncoding = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, blob);
    }

    #[test]
    fn encoding_pack() {
        let blob = sample_blob_encoding();
        let packed = blob.pack();
        let recovered = BlobEncoding::unpack(packed);

        assert_eq!(recovered.stripe_size, StorageUnits::from_bytes(64));
        assert_eq!(recovered.stripe_count, StripeCount(2));
        assert_eq!(recovered, blob);
    }
}
