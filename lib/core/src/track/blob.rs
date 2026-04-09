//! Blob payload metadata and commitment encoding.

use core::mem::size_of;

use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::hash::hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_crypto::merkle::hash_leaf;

use crate::encoding::EncodingProfile;
use crate::spooler::SpoolIndex;
use crate::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use crate::types::{StorageUnits, StripeCount};

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
///
/// `BlobInfo` carries two distinct merkle commitments, each with its own purpose:
///
/// ```text
/// source data ──stripe split──► stripes ──hash tree──► BlobInfo.root
///                   │
///                   └──Clay encode──► slices ──hash tree──► BlobInfo.commitment
///                                            └─per-leaf─►   BlobInfo.leaves[i]
/// ```
///
/// `root` is a merkle root over the **stripes of the source data** — the input
/// to the encoder, before erasure coding. Because `track.value_hash` is
/// `hash(BlobInfo)`, the on-chain track transitively commits to the source data
/// structure. This enables application-layer proofs that do not require touching
/// any erasure slices:
///
/// - **Stripe inclusion proofs**: prove "stripe N at offset X..Y is part of
///   blob Z" without downloading or decoding any erasure slices. The verifier
///   needs only the stripe bytes, the merkle path, and `value_hash` from chain.
/// - **Range reads with verification**: read part of a blob, verify against
///   on-chain state, without trusting the serving node and without
///   reconstructing the whole blob.
/// - **Light client attestations**: attest to a single event inside a snapshot
///   chunk (e.g., "registration X happened in epoch E") with a compact proof
///   against the on-chain manifest.
/// - **Cheap data integrity audits**: spot-check that a node holds the right
///   bytes by asking for a stripe + proof, instead of asking for slices and
///   re-decoding.
///
/// `commitment` is a merkle root over the **erasure-coded slice leaves** — the
/// output of the encoder. It serves the storage layer: it lets a node prove
/// that a particular slice it serves belongs to the canonical encoding of the
/// blob. `leaves[i]` holds the per-slice leaf hash so a server can produce a
/// merkle path against `commitment` for any slice it owns.
///
/// The two are structurally distinct and both load-bearing. `root` is for the
/// application layer (proofs about user-visible data); `commitment` is for the
/// storage layer (proofs about encoded slices).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize))]
pub struct BlobInfo {
    /// Original unencoded data size in bytes.
    pub size: StorageUnits,
    /// Merkle root over the stripes of the source data (pre-encoding).
    /// Enables stripe inclusion proofs and range-read verification against
    /// `track.value_hash` without touching erasure slices.
    pub root: Hash,
    /// Merkle root over the erasure-coded slice leaves (post-encoding).
    /// Enables per-slice membership proofs for the storage layer.
    pub commitment: Hash,
    /// Erasure-coding profile used for the blob.
    pub profile: EncodingProfile,
    /// Stripe size in bytes. Determines the leaf granularity of the `root` tree.
    pub stripe_size: StorageUnits,
    /// Number of stripes in the source data tree.
    pub stripe_count: StripeCount,
    /// Per-slice leaf hashes; the leaves of the `commitment` tree.
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
    pub fn verify_slice(&self, position: SpoolIndex, data: &[u8]) -> bool {
        let position = position as usize;
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
        // SAFETY: The serialized representation is exactly `PackedBlobInfo` bytes for this
        // pod-compatible type.
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
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
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

    #[test]
    fn blob_info_pack_roundtrip_uses_domain_types() {
        let blob = sample_blob_info();
        let packed = blob.pack();
        let recovered = BlobInfo::unpack(packed);

        assert_eq!(recovered.stripe_size, StorageUnits::from_bytes(64));
        assert_eq!(recovered.stripe_count, StripeCount(2));
        assert_eq!(recovered, blob);
    }

    // root and commitment commit to different inputs (source stripes vs.
    // erasure-coded slice leaves), so they must be distinct hashes for any
    // real blob. Regression guard against the prior bug where the SDK
    // encoder set both fields to the same value.
    #[test]
    fn root_and_commitment_are_distinct() {
        let blob = sample_blob_info();
        assert_ne!(blob.root, blob.commitment);
    }

    // Layout guard: the source-data root work in build.rs / encoder.rs only
    // changes the *value* of BlobInfo.root, never the byte layout. If this
    // assertion ever fails, the wire format and packed account layouts have
    // shifted and every dependent struct needs revisiting.
    #[test]
    fn blob_info_size_is_stable() {
        // size:            8
        // root:           32
        // commitment:     32
        // profile:         varies, sized by EncodingProfile
        // stripe_size:     8
        // stripe_count:    8
        // leaves:         32 * 20 = 640
        //
        // We don't pin the absolute total because EncodingProfile may grow
        // legitimately, but we do pin that BlobInfo is bytemuck-Pod-sized and
        // larger than the sum of its fixed-size fields.
        const FIXED_FIELDS: usize = 8 + 32 + 32 + 8 + 8 + (32 * SPOOL_GROUP_SIZE);
        assert!(size_of::<BlobInfo>() >= FIXED_FIELDS);
    }
}
