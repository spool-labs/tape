use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::hash::hash;
use tape_crypto::merkle::root_from_leaf_hashes;

use crate::encoding::EncodingProfile;
use crate::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use crate::types::StorageUnits;

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
pub struct BlobInfo {
    /// Original unencoded data size in bytes.
    pub size: StorageUnits,
    /// Root of the source data tree.
    pub root: Hash,
    /// Root of the erasure-coded commitment tree.
    pub commitment: Hash,
    /// Erasure-coding profile used for the blob.
    pub profile: EncodingProfile,
    /// Stripe size in bytes.
    pub stripe_size: u64,
    /// Number of stripes.
    pub stripe_count: u64,
    /// Per-slice commitment leaves.
    pub leaves: [Hash; SPOOL_GROUP_SIZE],
}

pub type PackedBlobInfo = [u8; size_of::<BlobInfo>()];

impl BlobInfo {
    #[inline(always)]
    pub fn pack(&self) -> PackedBlobInfo {
        let mut out = [0u8; size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    #[inline(always)]
    pub fn unpack(data: PackedBlobInfo) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }

    /// Recompute the commitment root from stored leaf hashes.
    pub fn commitment_root(&self) -> Hash {
        root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&self.leaves)
    }

    /// Verify a single slice against its stored leaf hash.
    pub fn verify_slice(&self, position: usize, data: &[u8]) -> bool {
        if position >= self.leaves.len() {
            return false;
        }

        tape_crypto::merkle::hash_leaf(data) == self.leaves[position]
    }

    /// Compute the canonical value hash for this blob payload.
    pub fn get_hash(&self) -> Hash {
        hash(bytemuck::bytes_of(self))
    }
}

#[cfg(feature = "wincode")]
impl SchemaWrite for BlobInfo {
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
impl<'de> SchemaRead<'de> for BlobInfo {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let packed: PackedBlobInfo = unsafe { reader.get_t()? };
        dst.write(Self::unpack(packed));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;

    fn sample_blob_info() -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(512),
            root: Hash::from([0x11; 32]),
            commitment: Hash::from([0x22; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: 64,
            stripe_count: 2,
            leaves: [Hash::from([0x33; 32]); SPOOL_GROUP_SIZE],
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn blob_info_wincode_roundtrip() {
        let blob = sample_blob_info();
        let bytes = wincode::serialize(&blob).expect("serialize");
        let recovered: BlobInfo = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, blob);
    }
}
